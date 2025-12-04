import asyncio
import logging
import sqlite3
from contextlib import closing
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, ChatMemberUpdatedFilter
from aiogram.types import (
    Message,
    ChatMemberUpdated,
    ChatInviteLink,
)
from aiogram.client.default import DefaultBotProperties


BOT_TOKEN = "7725677007:AAELRuzM3MLnrWyi74PeWZgJDyqkwHzPPEo"
CHANNEL_USERNAME = "mak8imrabota"  # @имя канала без @
ADMIN_ID = 1576058332

MIN_WITHDRAW = 30
REF_BONUS = 3

DB_PATH = Path("bot.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def init_db() -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                balance REAL NOT NULL DEFAULT 0,
                invite_link TEXT,
                ref_code TEXT UNIQUE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS referrals (
                invited_user_id INTEGER PRIMARY KEY,
                inviter_id INTEGER NOT NULL,
                active INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (inviter_id) REFERENCES users(user_id)
            )
            """
        )
        conn.commit()


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_or_create_user(user_id: int):
    with closing(get_db_connection()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        if row:
            return row
        # Создаем реферальный код
        ref_code = f"ref{user_id}"
        cur.execute(
            "INSERT INTO users (user_id, balance, invite_link, ref_code) VALUES (?, 0, NULL, ?)",
            (user_id, ref_code),
        )
        conn.commit()
        cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return cur.fetchone()


def set_user_invite_link(user_id: int, link: str):
    with closing(get_db_connection()) as conn:
        conn.execute(
            "UPDATE users SET invite_link = ? WHERE user_id = ?",
            (link, user_id),
        )
        conn.commit()


def get_user_by_ref_code(ref_code: str):
    """Находим пользователя по реферальному коду."""
    with closing(get_db_connection()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE ref_code = ?", (ref_code,))
        row = cur.fetchone()
        return int(row["user_id"]) if row else None


def get_ref_code_by_user_id(user_id: int):
    """Получаем реферальный код пользователя."""
    with closing(get_db_connection()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT ref_code FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return row["ref_code"] if row else None


def change_balance(user_id: int, delta: float):
    with closing(get_db_connection()) as conn:
        conn.execute(
            "UPDATE users SET balance = balance + ? WHERE user_id = ?",
            (delta, user_id),
        )
        conn.commit()


def get_balance(user_id: int) -> float:
    with closing(get_db_connection()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return float(row["balance"]) if row else 0.0


def set_referral_on_join(invited_user_id: int, inviter_id: int, pay: bool = True):
    """Обновляем/создаём реферальную связь при вступлении."""
    with closing(get_db_connection()) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT inviter_id, active FROM referrals WHERE invited_user_id = ?",
            (invited_user_id,),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO referrals (invited_user_id, inviter_id, active) VALUES (?, ?, 1)",
                (invited_user_id, inviter_id),
            )
            if pay:
                cur.execute(
                    "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                    (REF_BONUS, inviter_id),
                )
        else:
            prev_active = bool(row["active"])
            prev_inviter_id = int(row["inviter_id"])
            cur.execute(
                "UPDATE referrals SET inviter_id = ?, active = 1 WHERE invited_user_id = ?",
                (inviter_id, invited_user_id),
            )
            # Если ранее был активный реферал у другого пригласившего, снимаем с него,
            # а новому пригласившему начисляем.
            if prev_active and prev_inviter_id != inviter_id:
                cur.execute(
                    "UPDATE users SET balance = balance - ? WHERE user_id = ?",
                    (REF_BONUS, prev_inviter_id),
                )
                cur.execute(
                    "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                    (REF_BONUS, inviter_id),
                )
            elif not prev_active and pay:
                # Повторное вступление после выхода
                cur.execute(
                    "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                    (REF_BONUS, inviter_id),
                )
        conn.commit()


def set_referral_on_leave(invited_user_id: int):
    """Обновляем реферальную связь при выходе."""
    with closing(get_db_connection()) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT inviter_id, active FROM referrals WHERE invited_user_id = ?",
            (invited_user_id,),
        )
        row = cur.fetchone()
        if row is None:
            return
        if not bool(row["active"]):
            return
        inviter_id = int(row["inviter_id"])
        cur.execute(
            "UPDATE referrals SET active = 0 WHERE invited_user_id = ?",
            (invited_user_id,),
        )
        cur.execute(
            "UPDATE users SET balance = balance - ? WHERE user_id = ?",
            (REF_BONUS, inviter_id),
        )
        conn.commit()


def find_inviter_by_link(link: str):
    with closing(get_db_connection()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE invite_link = ?", (link,))
        row = cur.fetchone()
        return int(row["user_id"]) if row else None


async def find_inviter_by_link_id(bot: Bot, invite_link_id: str):
    """Находим пригласившего по invite_link_id через проверку всех ссылок пользователей."""
    if not invite_link_id:
        return None
    
    with closing(get_db_connection()) as conn:
        cur = conn.cursor()
        # Получаем все ссылки из базы
        cur.execute("SELECT user_id, invite_link FROM users WHERE invite_link IS NOT NULL")
        rows = cur.fetchall()
        
        for row in rows:
            link = row["invite_link"]
            try:
                # Пытаемся получить информацию о ссылке через API
                # Но это может не работать, так как нет метода для получения ссылки по ID
                # Поэтому просто проверяем по полному тексту ссылки
                if invite_link_id in link or link.endswith(invite_link_id):
                    return int(row["user_id"])
            except:
                pass
    
    return None


async def create_or_get_invite_link(bot: Bot, user_id: int) -> str:
    """Создаем или получаем прямую пригласительную ссылку на канал."""
    user = get_or_create_user(user_id)
    if user["invite_link"]:
        logger.info(f"Возвращаем существующую ссылку для user_id={user_id}: {user['invite_link']}")
        return user["invite_link"]

    # Создаём индивидуальную пригласительную ссылку на канал
    try:
        invite: ChatInviteLink = await bot.create_chat_invite_link(
            chat_id=f"@{CHANNEL_USERNAME}",
            name=f"ref_{user_id}",
            creates_join_request=False,
        )
        logger.info(f"Создана новая ссылка канала для user_id={user_id}: {invite.invite_link}, invite_link_id={invite.invite_link_id}")
    except Exception as e:
        logger.exception("Не удалось создать пригласительную ссылку: %s", e)
        raise

    set_user_invite_link(user_id, invite.invite_link)
    return invite.invite_link


dp = Dispatcher()


@dp.message(Command("start"))
async def cmd_start(message: Message, bot: Bot):
    user_id = message.from_user.id
    get_or_create_user(user_id)

    try:
        invite_link = await create_or_get_invite_link(bot, user_id)
    except Exception:
        await message.answer(
            "Произошла ошибка при создании вашей пригласительной ссылки. "
            "Напишите, пожалуйста, администратору."
        )
        return

    balance = get_balance(user_id)
    
    # Подсчитываем активных рефералов
    with closing(get_db_connection()) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM referrals WHERE inviter_id = ? AND active = 1",
            (user_id,)
        )
        active_refs = cur.fetchone()[0]

    text = (
        "👋 Привет! Это бот для заработка на приглашении подписчиков в канал.\n\n"
        "⭐ За каждого нового пользователя, который вступит в канал по твоей ссылке, "
        f"ты получаешь {REF_BONUS} рубля.\n"
        f"❌ Если приглашённый отпишется от канала, с твоего баланса спишется {REF_BONUS} рубля.\n\n"
        f"💰 Минимальная сумма вывода: {MIN_WITHDRAW} рублей.\n"
        "💡 Оплата начисляется только за реальных пользователей, не накрученных.\n"
        "🔐 Вывод средств осуществляется через администратора.\n\n"
        "В бота может зайти любой пользователь — он служит для заработка.\n\n"
        "📩 Твоя индивидуальная пригласительная ссылка:\n"
        f"{invite_link}\n\n"
        "📢 Просто поделись ссылкой! Когда кто-то подпишется на канал по твоей ссылке, "
        f"тебе автоматически начислится {REF_BONUS} рубля.\n\n"
        f"📊 Текущий баланс: {balance:.2f} ₽\n"
        f"👥 Активных рефералов: {active_refs}\n\n"
        "Команды:\n"
        "/balance — показать баланс и рефералов\n"
        "/refs — список всех рефералов\n"
        "/withdraw — запросить вывод средств"
    )

    await message.answer(text)


@dp.message(Command("balance"))
async def cmd_balance(message: Message):
    user_id = message.from_user.id
    get_or_create_user(user_id)
    balance = get_balance(user_id)
    
    # Подсчитываем активных рефералов
    with closing(get_db_connection()) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM referrals WHERE inviter_id = ? AND active = 1",
            (user_id,)
        )
        active_refs = cur.fetchone()[0]
    
    text = (
        f"📊 Ваш текущий баланс: {balance:.2f} ₽\n"
        f"👥 Активных рефералов: {active_refs}"
    )
    await message.answer(text)


@dp.message(Command("refs"))
async def cmd_refs(message: Message, bot: Bot):
    """Показать подробный список всех рефералов пользователя."""
    user_id = message.from_user.id
    get_or_create_user(user_id)
    
    with closing(get_db_connection()) as conn:
        cur = conn.cursor()
        # Получаем всех рефералов
        cur.execute(
            "SELECT invited_user_id, active FROM referrals WHERE inviter_id = ? ORDER BY invited_user_id",
            (user_id,)
        )
        rows = cur.fetchall()
    
    if not rows:
        await message.answer("У вас пока нет рефералов.\nПоделитесь своей реферальной ссылкой!")
        return
    
    active_count = sum(1 for row in rows if bool(row["active"]))
    inactive_count = len(rows) - active_count
    
    text = f"👥 <b>Ваши рефералы:</b>\n\n"
    text += f"✅ Активных (подписаны): {active_count}\n"
    text += f"❌ Неактивных (отписались): {inactive_count}\n"
    text += f"📊 Всего приглашено: {len(rows)}\n\n"
    text += f"<b>Детальный список:</b>\n"
    
    # Показываем всех рефералов с их статусом
    for i, row in enumerate(rows, 1):
        ref_user_id = row["invited_user_id"]
        is_active = bool(row["active"])
        status = "✅ Подписан" if is_active else "❌ Отписался"
        
        # Пытаемся получить информацию о пользователе
        try:
            ref_user = await bot.get_chat(ref_user_id)
            username = f"@{ref_user.username}" if ref_user.username else "нет username"
            name = ref_user.first_name or "Неизвестно"
            text += f"{i}. {status}\n"
            text += f"   👤 {name} ({username})\n"
            text += f"   🆔 ID: <code>{ref_user_id}</code>\n\n"
        except:
            text += f"{i}. {status}\n"
            text += f"   🆔 ID: <code>{ref_user_id}</code>\n\n"
        
        # Ограничение длины сообщения Telegram
        if len(text) > 3500:
            await message.answer(text, parse_mode="HTML")
            text = f"<b>Продолжение списка:</b>\n\n"
    
    if text and len(text) > 20:
        await message.answer(text, parse_mode="HTML")


@dp.message(Command("withdraw"))
async def cmd_withdraw(message: Message, bot: Bot):
    user_id = message.from_user.id
    get_or_create_user(user_id)
    balance = get_balance(user_id)

    if balance < MIN_WITHDRAW:
        await message.answer(
            f"Минимальная сумма для вывода — {MIN_WITHDRAW} ₽.\n"
            f"Сейчас на вашем балансе: {balance:.2f} ₽.\n"
            "Продолжайте приглашать новых реальных пользователей!",
        )
        return

    await message.answer(
        "✅ Ваша заявка на вывод отправлена администратору.\n"
        "Ожидайте, админ свяжется с вами для выплаты.\n\n"
        "Напоминаем: оплата начисляется только за реальных пользователей, не накрученных.",
    )

    user = message.from_user
    mention = user.mention_html() if hasattr(user, "mention_html") else f"id {user_id}"

    text = (
        f"💸 <b>Новая заявка на вывод</b>\n\n"
        f"Пользователь: {mention}\n"
        f"User ID: <code>{user_id}</code>\n"
        f"Username: @{user.username if user.username else 'нет'}\n"
        f"Текущий баланс: <b>{balance:.2f} ₽</b>\n\n"
        f"Минимальный порог вывода: {MIN_WITHDRAW} ₽\n"
        "После выплаты не забудьте вручную скорректировать баланс при необходимости."
    )

    try:
        await bot.send_message(chat_id=ADMIN_ID, text=text, parse_mode="HTML")
    except Exception as e:
        logger.exception("Не удалось отправить сообщение админу: %s", e)


@dp.message(Command("stats"))
async def cmd_stats(message: Message, bot: Bot):
    """Команда для администратора - проверка статистики ссылок."""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        # Получаем информацию о канале и ссылках
        chat = await bot.get_chat(f"@{CHANNEL_USERNAME}")
        await message.answer(f"📊 Канал: {chat.title} (ID: {chat.id})")
        
        # Получаем все ссылки из базы
        with closing(get_db_connection()) as conn:
            cur = conn.cursor()
            cur.execute("SELECT user_id, invite_link FROM users WHERE invite_link IS NOT NULL")
            rows = cur.fetchall()
            
            if not rows:
                await message.answer("В базе нет созданных ссылок.")
                return
            
            text = f"📋 <b>Всего ссылок в базе: {len(rows)}</b>\n\n"
            for row in rows:
                user_id = row["user_id"]
                link = row["invite_link"]
                balance = get_balance(user_id)
                # Подсчитываем активных и неактивных рефералов
                cur.execute(
                    "SELECT COUNT(*) FROM referrals WHERE inviter_id = ? AND active = 1",
                    (user_id,)
                )
                active_count = cur.fetchone()[0]
                cur.execute(
                    "SELECT COUNT(*) FROM referrals WHERE inviter_id = ? AND active = 0",
                    (user_id,)
                )
                inactive_count = cur.fetchone()[0]
                
                try:
                    user_info = await bot.get_chat(user_id)
                    username = f"@{user_info.username}" if user_info.username else "нет username"
                    name = user_info.first_name or "Неизвестно"
                except:
                    username = "недоступен"
                    name = "Неизвестно"
                
                text += f"👤 <b>{name}</b> ({username})\n"
                text += f"🆔 ID: <code>{user_id}</code>\n"
                text += f"💰 Баланс: {balance:.2f}₽\n"
                text += f"✅ Активных рефералов: {active_count}\n"
                text += f"❌ Неактивных: {inactive_count}\n"
                text += f"📎 Ссылка: {link}\n\n"
                
                if len(text) > 3500:
                    await message.answer(text, parse_mode="HTML")
                    text = ""
            
            if text:
                await message.answer(text, parse_mode="HTML")
            
    except Exception as e:
        logger.exception("Ошибка при получении статистики: %s", e)
        await message.answer(f"Ошибка: {e}")


@dp.message(Command("checklinks"))
async def cmd_checklinks(message: Message, bot: Bot):
    """Команда для администратора - проверка всех ссылок в базе."""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        with closing(get_db_connection()) as conn:
            cur = conn.cursor()
            cur.execute("SELECT user_id, invite_link FROM users WHERE invite_link IS NOT NULL")
            rows = cur.fetchall()
            
            if not rows:
                await message.answer("В базе нет ссылок.")
                return
            
            text = f"🔍 <b>Все ссылки в базе ({len(rows)} шт.):</b>\n\n"
            for i, row in enumerate(rows, 1):
                text += f"{i}. User {row['user_id']}:\n{row['invite_link']}\n\n"
                if len(text) > 3500:  # Ограничение Telegram
                    await message.answer(text, parse_mode="HTML")
                    text = ""
            
            if text:
                await message.answer(text, parse_mode="HTML")
            
    except Exception as e:
        logger.exception("Ошибка при проверке ссылок: %s", e)
        await message.answer(f"Ошибка: {e}")


@dp.message(Command("allrefs"))
async def cmd_allrefs(message: Message, bot: Bot):
    """Команда для администратора - показать все реферальные связи."""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        with closing(get_db_connection()) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT inviter_id, invited_user_id, active FROM referrals ORDER BY inviter_id, invited_user_id"
            )
            rows = cur.fetchall()
            
            if not rows:
                await message.answer("В базе нет реферальных связей.")
                return
            
            text = f"🔗 <b>Все реферальные связи ({len(rows)} шт.):</b>\n\n"
            
            for row in rows:
                inviter_id = row["inviter_id"]
                invited_id = row["invited_user_id"]
                is_active = bool(row["active"])
                status = "✅ Подписан" if is_active else "❌ Отписался"
                
                try:
                    inviter = await bot.get_chat(inviter_id)
                    inviter_name = inviter.first_name or "Неизвестно"
                    invited = await bot.get_chat(invited_id)
                    invited_name = invited.first_name or "Неизвестно"
                    
                    text += f"👤 <b>{inviter_name}</b> (ID: {inviter_id})\n"
                    text += f"   → пригласил →\n"
                    text += f"👤 <b>{invited_name}</b> (ID: {invited_id}) - {status}\n\n"
                except:
                    text += f"👤 User {inviter_id} → пригласил → User {invited_id} - {status}\n\n"
                
                if len(text) > 3500:
                    await message.answer(text, parse_mode="HTML")
                    text = ""
            
            if text:
                await message.answer(text, parse_mode="HTML")
            
    except Exception as e:
        logger.exception("Ошибка при получении всех рефералов: %s", e)
        await message.answer(f"Ошибка: {e}")


# Сохраняем ID канала для проверки
CHANNEL_ID = None

async def get_channel_id(bot: Bot) -> int | None:
    """Получаем ID канала для проверки событий."""
    global CHANNEL_ID
    if CHANNEL_ID:
        return CHANNEL_ID
    try:
        chat = await bot.get_chat(f"@{CHANNEL_USERNAME}")
        CHANNEL_ID = chat.id
        logger.info(f"Channel ID сохранен: {CHANNEL_ID}")
        return CHANNEL_ID
    except Exception as e:
        logger.exception(f"Не удалось получить ID канала: {e}")
        return None


@dp.chat_member(
    ChatMemberUpdatedFilter(
        member_status_changed=True,
    )
)
async def on_chat_member_update(event: ChatMemberUpdated, bot: Bot):
    """
    Отслеживаем вступления и выходы из канала.
    Теперь используем сохраненную связь реферал-пригласивший из базы данных.
    """
    chat = event.chat
    
    # Получаем ID канала для проверки
    channel_id = await get_channel_id(bot)
    
    # Проверяем, что событие из нашего канала
    is_our_channel = False
    if chat.username and chat.username.lower() == CHANNEL_USERNAME.lower():
        is_our_channel = True
    elif channel_id and chat.id == channel_id:
        is_our_channel = True
    
    if not is_our_channel:
        logger.debug(f"Событие из другого чата: username={chat.username}, id={chat.id}")
        return

    old_status = event.old_chat_member.status
    new_status = event.new_chat_member.status
    user_id = event.from_user.id

    logger.info(
        f"🔔 Chat member update: user_id={user_id}, chat_id={chat.id}, "
        f"old_status={old_status}, new_status={new_status}"
    )

    # Вступление в канал
    if new_status in ("member", "administrator") and old_status in ("left", "kicked", "restricted"):
        logger.info(f"✅ User {user_id} JOINED channel")
        
        # Получаем информацию о ссылке, по которой пользователь вступил
        invite: ChatInviteLink | None = event.invite_link
        
        if not invite:
            logger.warning(f"⚠️ User {user_id} joined channel but invite_link is None - Telegram не передал информацию о ссылке")
            logger.warning(f"Это может означать, что пользователь вступил напрямую или через другую ссылку")
            return
        
        link_str = invite.invite_link
        invite_link_id = getattr(invite, 'invite_link_id', None)
        logger.info(f"📎 User {user_id} joined via link: {link_str}, invite_link_id={invite_link_id}")
        
        # Выводим все ссылки из базы для отладки
        with closing(get_db_connection()) as conn:
            cur = conn.cursor()
            cur.execute("SELECT user_id, invite_link FROM users WHERE invite_link IS NOT NULL")
            all_links = cur.fetchall()
            logger.info(f"🔍 Всего ссылок в базе: {len(all_links)}")
            for link_row in all_links[:5]:  # Показываем первые 5
                logger.info(f"  - User {link_row['user_id']}: {link_row['invite_link']}")
        
        # Ищем пригласившего по ссылке канала
        inviter_id = find_inviter_by_link(link_str)
        
        if not inviter_id:
            logger.error(f"❌ Ссылка {link_str} не найдена в базе данных!")
            logger.error(f"Проверьте, что бот является администратором канала с правами на управление ссылками")
            logger.error(f"Также проверьте, что ссылка была создана через этого бота")
            return
        
        # Проверяем, не был ли уже этот пользователь активным рефералом
        with closing(get_db_connection()) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT inviter_id, active FROM referrals WHERE invited_user_id = ?",
                (user_id,)
            )
            row = cur.fetchone()
        
        was_active = False
        if row:
            was_active = bool(row["active"])
            prev_inviter_id = int(row["inviter_id"])
            # Если пользователь уже был активным рефералом другого пригласившего
            if was_active and prev_inviter_id != inviter_id:
                logger.info(f"User {user_id} был активным рефералом {prev_inviter_id}, теперь переходит к {inviter_id}")
        
        # Активируем реферальную связь и начисляем бонус
        get_or_create_user(inviter_id)
        set_referral_on_join(invited_user_id=user_id, inviter_id=inviter_id, pay=True)
        
        new_balance = get_balance(inviter_id)
        logger.info(
            f"💰 User {user_id} joined channel via inviter {inviter_id}, +{REF_BONUS} руб. "
            f"Новый баланс пригласившего: {new_balance:.2f} ₽"
        )
        
        # Отправляем уведомление пригласившему
        try:
            invited_user = await bot.get_chat(user_id)
            invited_name = invited_user.first_name or "Пользователь"
            invited_username = f"@{invited_user.username}" if invited_user.username else "нет username"
            
            notification_text = (
                f"🎉 <b>Новый реферал подписался!</b>\n\n"
                f"👤 <b>{invited_name}</b> ({invited_username})\n"
                f"🆔 ID: <code>{user_id}</code>\n\n"
                f"💰 Вам начислено: <b>+{REF_BONUS} ₽</b>\n"
                f"💵 Ваш баланс: <b>{new_balance:.2f} ₽</b>\n\n"
                f"Используйте /refs чтобы посмотреть всех рефералов"
            )
            await bot.send_message(chat_id=inviter_id, text=notification_text, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление пригласившему {inviter_id}: {e}")

    # Выход из канала
    elif old_status in ("member", "administrator") and new_status in ("left", "kicked"):
        logger.info(f"❌ User {user_id} LEFT channel")
        
        # Находим пригласившего перед списанием
        with closing(get_db_connection()) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT inviter_id, active FROM referrals WHERE invited_user_id = ?",
                (user_id,)
            )
            row = cur.fetchone()
        
        if row and bool(row["active"]):
            inviter_id = int(row["inviter_id"])
            old_balance = get_balance(inviter_id)
            
            # Списываем средства
            set_referral_on_leave(invited_user_id=user_id)
            
            new_balance = get_balance(inviter_id)
            logger.info(
                f"❌ User {user_id} left channel, inviter {inviter_id} lost {REF_BONUS} руб. "
                f"Новый баланс: {new_balance:.2f} ₽"
            )
            
            # Отправляем уведомление пригласившему
            try:
                left_user = await bot.get_chat(user_id)
                left_name = left_user.first_name or "Пользователь"
                left_username = f"@{left_user.username}" if left_user.username else "нет username"
                
                notification_text = (
                    f"⚠️ <b>Реферал отписался от канала</b>\n\n"
                    f"👤 <b>{left_name}</b> ({left_username})\n"
                    f"🆔 ID: <code>{user_id}</code>\n\n"
                    f"💰 С вашего баланса списано: <b>-{REF_BONUS} ₽</b>\n"
                    f"💵 Ваш баланс: <b>{new_balance:.2f} ₽</b>\n\n"
                    f"Используйте /refs чтобы посмотреть всех рефералов"
                )
                await bot.send_message(chat_id=inviter_id, text=notification_text, parse_mode="HTML")
            except Exception as e:
                logger.warning(f"Не удалось отправить уведомление пригласившему {inviter_id}: {e}")
        else:
            logger.info(f"User {user_id} left channel but was not an active referral")


async def main():
    init_db()
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode="HTML"),
    )
    logger.info("Bot started")
    await dp.start_polling(bot, allowed_updates=["message", "chat_member"])


if __name__ == "__main__":
    asyncio.run(main())


