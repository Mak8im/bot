import asyncio
import logging
import sqlite3
from typing import Dict, Optional
from datetime import datetime

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

# Конфигурация
BOT_TOKEN = "7725677007:AAELRuzM3MLnrWyi74PeWZgJDyqkwHzPPEo"
CHANNEL_USERNAME = "mak8imrabota"  # без @
ADMIN_ID = 1576058332
DATABASE_NAME = "invite_bot.db"

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация базы данных
def init_db():
    with sqlite3.connect(DATABASE_NAME) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                balance REAL DEFAULT 0,
                invite_link TEXT UNIQUE,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS invited_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                inviter_id INTEGER,
                invited_user_id INTEGER UNIQUE,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                left_at TIMESTAMP,
                FOREIGN KEY (inviter_id) REFERENCES users (user_id)
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount REAL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        conn.commit()

# Генерация уникальной пригласительной ссылки
async def create_invite_link(user_id: int) -> str:
    try:
        chat = await bot.get_chat(f"@{CHANNEL_USERNAME}")
        # Создаем ссылку с пометкой о пригласившем
        link = await bot.create_chat_invite_link(
            chat_id=chat.id,
            name=f"Приглашение от {user_id}",
            creates_join_request=False
        )
        return link.invite_link
    except Exception as e:
        logger.error(f"Ошибка при создании ссылки: {e}")
        # Если не удалось создать через API, создаем кастомную ссылку
        return f"https://t.me/{CHANNEL_USERNAME}?start=ref{user_id}"

# Получение пользователя из БД
def get_user(user_id: int):
    with sqlite3.connect(DATABASE_NAME) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

# Регистрация нового пользователя
async def register_user(user_id: int, username: str, first_name: str):
    with sqlite3.connect(DATABASE_NAME) as conn:
        user = get_user(user_id)
        if not user:
            invite_link = await create_invite_link(user_id)
            conn.execute(
                'INSERT INTO users (user_id, username, first_name, invite_link) VALUES (?, ?, ?, ?)',
                (user_id, username, first_name, invite_link)
            )
            conn.commit()
            return invite_link
        return user.get('invite_link')

# Получение баланса
def get_balance(user_id: int) -> float:
    user = get_user(user_id)
    return user['balance'] if user else 0

# Обновление баланса
def update_balance(user_id: int, amount: float):
    with sqlite3.connect(DATABASE_NAME) as conn:
        conn.execute(
            'UPDATE users SET balance = balance + ? WHERE user_id = ?',
            (amount, user_id)
        )
        conn.commit()

# Проверка, является ли пользователь участником канала
async def is_channel_member(user_id: int) -> bool:
    try:
        chat = await bot.get_chat(f"@{CHANNEL_USERNAME}")
        member = await bot.get_chat_member(chat.id, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except:
        return False

# Обработчик команды /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    
    # Регистрируем пользователя
    invite_link = await register_user(user_id, username, first_name)
    
    # Проверяем реферальную ссылку
    args = message.text.split()
    if len(args) > 1 and args[1].startswith('ref'):
        try:
            inviter_id = int(args[1][3:])
            if inviter_id != user_id:
                await handle_referral(user_id, inviter_id)
        except:
            pass
    
    # Создаем клавиатуру
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📢 Перейти в канал", url=f"https://t.me/{CHANNEL_USERNAME}"),
        InlineKeyboardButton(text="💳 Баланс", callback_data="balance")
    )
    builder.row(
        InlineKeyboardButton(text="👥 Пригласить друзей", callback_data="invite"),
        InlineKeyboardButton(text="💰 Вывод средств", callback_data="withdraw")
    )
    builder.row(
        InlineKeyboardButton(text="📊 Статистика", callback_data="stats")
    )
    
    await message.answer(
        f"👋 Привет, {first_name}!\n\n"
        f"🔗 <b>Твоя персональная ссылка для приглашений:</b>\n"
        f"<code>{invite_link}</code>\n\n"
        f"💵 <b>Заработок:</b>\n"
        f"• За каждого приглашенного: +3 рубля\n"
        f"• Если приглашенный отпишется: -3 рубля\n"
        f"• Минимальная сумма вывода: 30 рублей\n\n"
        f"⚠️ <b>Внимание:</b>\n"
        f"• Оплата начисляется только за реальных пользователей\n"
        f"• Вывод средств осуществляется через администратора\n"
        f"• Начисления происходят после вступления пользователя в канал",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )

# Обработка реферала
async def handle_referral(invited_user_id: int, inviter_id: int):
    # Проверяем, не регистрировался ли уже этот пользователь
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT * FROM invited_users WHERE invited_user_id = ?', 
            (invited_user_id,)
        )
        if cursor.fetchone():
            return
        
        # Регистрируем приглашение
        conn.execute(
            '''INSERT INTO invited_users (inviter_id, invited_user_id) 
               VALUES (?, ?)''',
            (inviter_id, invited_user_id)
        )
        conn.commit()
        
    # Проверяем, вступил ли пользователь в канал
    if await is_channel_member(invited_user_id):
        update_balance(inviter_id, 3)
        logger.info(f"Начислено 3 рубля пользователю {inviter_id} за приглашение {invited_user_id}")

# Обработчик кнопки "Баланс"
@dp.callback_query(F.data == "balance")
async def show_balance(callback: types.CallbackQuery):
    balance = get_balance(callback.from_user.id)
    invited_count = get_invited_count(callback.from_user.id)
    
    text = (
        f"💳 <b>Ваш баланс:</b> {balance:.2f} руб.\n"
        f"👥 <b>Приглашено пользователей:</b> {invited_count}\n"
        f"💰 <b>Минимальный вывод:</b> 30 руб.\n\n"
    )
    
    if balance >= 30:
        text += "✅ Вы можете запросить вывод средств!"
    else:
        text += f"⏳ До минимальной суммы вывода осталось: {30 - balance:.2f} руб."
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")
        ]]),
        parse_mode="HTML"
    )
    await callback.answer()

# Получение количества приглашенных
def get_invited_count(user_id: int) -> int:
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT COUNT(*) FROM invited_users WHERE inviter_id = ?', 
            (user_id,)
        )
        result = cursor.fetchone()
        return result[0] if result else 0

# Обработчик кнопки "Пригласить друзей"
@dp.callback_query(F.data == "invite")
async def show_invite_link(callback: types.CallbackQuery):
    user = get_user(callback.from_user.id)
    if user:
        text = (
            f"🔗 <b>Ваша реферальная ссылка:</b>\n"
            f"<code>{user['invite_link']}</code>\n\n"
            f"📢 <b>Отправьте эту ссылку друзьям:</b>\n"
            f"1. Они переходят по ссылке\n"
            f"2. Нажимают 'Start' в боте\n"
            f"3. Вступают в канал\n"
            f"4. Вы получаете +3 рубля!\n\n"
            f"⚠️ <b>Важно:</b> Если приглашенный отпишется от канала, с вашего баланса будет списано 3 рубля."
        )
        
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Поделиться ссылкой", url=f"https://t.me/share/url?url={user['invite_link']}&text=Присоединяйся%20к%20каналу!")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
            ]),
            parse_mode="HTML"
        )
    await callback.answer()

# Обработчик кнопки "Вывод средств"
@dp.callback_query(F.data == "withdraw")
async def withdraw_funds(callback: types.CallbackQuery):
    balance = get_balance(callback.from_user.id)
    
    if balance < 30:
        await callback.message.edit_text(
            f"❌ <b>Недостаточно средств!</b>\n\n"
            f"💰 Ваш баланс: {balance:.2f} руб.\n"
            f"💰 Минимальная сумма вывода: 30 руб.\n\n"
            f"Пригласите ещё {int((30 - balance) / 3) + 1} друзей для вывода.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")
            ]]),
            parse_mode="HTML"
        )
        await callback.answer()
        return
    
    # Создаем заявку на вывод
    with sqlite3.connect(DATABASE_NAME) as conn:
        conn.execute(
            'INSERT INTO withdrawals (user_id, amount) VALUES (?, ?)',
            (callback.from_user.id, balance)
        )
        conn.commit()
    
    # Обнуляем баланс
    update_balance(callback.from_user.id, -balance)
    
    # Уведомляем администратора
    user_info = f"@{callback.from_user.username}" if callback.from_user.username else callback.from_user.first_name
    await bot.send_message(
        ADMIN_ID,
        f"🤑 <b>Новая заявка на вывод!</b>\n\n"
        f"👤 Пользователь: {user_info} (ID: {callback.from_user.id})\n"
        f"💰 Сумма: {balance:.2f} руб.\n"
        f"📅 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        parse_mode="HTML"
    )
    
    await callback.message.edit_text(
        f"✅ <b>Заявка на вывод создана!</b>\n\n"
        f"💰 Сумма: {balance:.2f} руб.\n"
        f"👤 Администратор свяжется с вами в течение 24 часов.\n\n"
        f"⚠️ <b>Внимание:</b> Вывод осуществляется только на карту РФ.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")
        ]]),
        parse_mode="HTML"
    )
    await callback.answer()

# Обработчик кнопки "Статистика"
@dp.callback_query(F.data == "stats")
async def show_stats(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    balance = get_balance(user_id)
    invited_count = get_invited_count(user_id)
    
    # Получаем активных приглашенных (которые сейчас в канале)
    active_count = 0
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''SELECT COUNT(*) FROM invited_users 
               WHERE inviter_id = ? AND left_at IS NULL''',
            (user_id,)
        )
        result = cursor.fetchone()
        active_count = result[0] if result else 0
    
    text = (
        f"📊 <b>Ваша статистика:</b>\n\n"
        f"💰 Баланс: {balance:.2f} руб.\n"
        f"👥 Всего приглашено: {invited_count}\n"
        f"✅ В канале сейчас: {active_count}\n"
        f"❌ Отписалось: {invited_count - active_count}\n\n"
        f"💵 <b>Расчет заработка:</b>\n"
        f"• Активные рефералы: {active_count} × 3 руб. = {active_count * 3} руб.\n"
        f"• Списания за отписки: {(invited_count - active_count) * 3} руб.\n"
    )
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")
        ]]),
        parse_mode="HTML"
    )
    await callback.answer()

# Обработчик кнопки "Назад"
@dp.callback_query(F.data == "main_menu")
async def main_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = get_user(user_id)
    
    if user:
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="📢 Перейти в канал", url=f"https://t.me/{CHANNEL_USERNAME}"),
            InlineKeyboardButton(text="💳 Баланс", callback_data="balance")
        )
        builder.row(
            InlineKeyboardButton(text="👥 Пригласить друзей", callback_data="invite"),
            InlineKeyboardButton(text="💰 Вывод средств", callback_data="withdraw")
        )
        builder.row(
            InlineKeyboardButton(text="📊 Статистика", callback_data="stats")
        )
        
        await callback.message.edit_text(
            f"👋 Добро пожаловать!\n\n"
            f"🔗 <b>Ваша персональная ссылка:</b>\n"
            f"<code>{user['invite_link']}</code>\n\n"
            f"💵 <b>Заработок:</b>\n"
            f"• За каждого приглашенного: +3 рубля\n"
            f"• Если приглашенный отпишется: -3 рубля\n"
            f"• Минимальная сумма вывода: 30 рублей\n\n"
            f"⚠️ <b>Внимание:</b>\n"
            f"• Оплата начисляется только за реальных пользователей\n"
            f"• Вывод средств осуществляется через администратора",
            reply_markup=builder.as_markup(),
            parse_mode="HTML"
        )
    await callback.answer()

# Периодическая проверка участников канала
async def check_channel_members():
    while True:
        try:
            # Получаем всех приглашенных пользователей
            with sqlite3.connect(DATABASE_NAME) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(
                    '''SELECT iu.id, iu.inviter_id, iu.invited_user_id, iu.left_at 
                       FROM invited_users iu
                       WHERE iu.left_at IS NULL'''
                )
                rows = cursor.fetchall()
                
                for row in rows:
                    user_id = row['invited_user_id']
                    inviter_id = row['inviter_id']
                    record_id = row['id']
                    
                    # Проверяем, состоит ли пользователь в канале
                    is_member = await is_channel_member(user_id)
                    
                    if not is_member:
                        # Пользователь вышел из канала
                        update_balance(inviter_id, -3)
                        conn.execute(
                            'UPDATE invited_users SET left_at = CURRENT_TIMESTAMP WHERE id = ?',
                            (record_id,)
                        )
                        logger.info(f"Списано 3 рубля с пользователя {inviter_id} за выход {user_id}")
                        conn.commit()
                    elif is_member and row['left_at']:
                        # Пользователь снова вступил (не должно быть, но на всякий случай)
                        update_balance(inviter_id, 3)
                        conn.execute(
                            'UPDATE invited_users SET left_at = NULL WHERE id = ?',
                            (record_id,)
                        )
                        logger.info(f"Начислено 3 рубля пользователю {inviter_id} за возврат {user_id}")
                        conn.commit()
        
        except Exception as e:
            logger.error(f"Ошибка при проверке участников: {e}")
        
        # Проверяем каждые 5 минут
        await asyncio.sleep(300)

# Команда для администратора
@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Доступ запрещен!")
        return
    
    # Получаем статистику
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        
        # Общее количество пользователей
        cursor.execute('SELECT COUNT(*) FROM users')
        total_users = cursor.fetchone()[0]
        
        # Общее количество приглашенных
        cursor.execute('SELECT COUNT(*) FROM invited_users')
        total_invited = cursor.fetchone()[0]
        
        # Активные приглашенные
        cursor.execute('SELECT COUNT(*) FROM invited_users WHERE left_at IS NULL')
        active_invited = cursor.fetchone()[0]
        
        # Ожидающие выплаты
        cursor.execute('SELECT COUNT(*) FROM withdrawals WHERE status = "pending"')
        pending_withdrawals = cursor.fetchone()[0]
        
        # Общая сумма выплат
        cursor.execute('SELECT SUM(amount) FROM withdrawals WHERE status = "pending"')
        pending_amount_result = cursor.fetchone()
        pending_amount = pending_amount_result[0] if pending_amount_result[0] else 0
    
    text = (
        f"📊 <b>Панель администратора</b>\n\n"
        f"👥 Всего пользователей: {total_users}\n"
        f"👥 Всего приглашенных: {total_invited}\n"
        f"✅ В канале сейчас: {active_invited}\n"
        f"❌ Отписалось: {total_invited - active_invited}\n\n"
        f"💰 Ожидает выплат: {pending_withdrawals} на сумму {pending_amount:.2f} руб.\n\n"
        f"Команды:\n"
        f"/stats - полная статистика\n"
        f"/withdrawals - список заявок на вывод\n"
        f"/users - список пользователей"
    )
    
    await message.answer(text, parse_mode="HTML")

# Запуск бота
async def main():
    # Инициализируем БД
    init_db()
    
    # Запускаем фоновую задачу проверки участников
    asyncio.create_task(check_channel_members())
    
    # Запускаем бота
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
