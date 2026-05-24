import asyncio
import logging
import os
import sys
import datetime
import random
import traceback
import re
import smtplib
import aiohttp
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from python_socks.sync import Proxy

from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, FSInputFile
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramUnauthorizedError
from telethon import TelegramClient
from telethon.tl.functions.account import ReportPeerRequest
from telethon.tl.functions.messages import DeleteRevokedExportedChatInvitesRequest
from telethon.tl.types import InputReportReasonIllegalDrugs, InputReportReasonOther, InputReportReasonPersonalDetails
from telethon.network import ConnectionTcpFull
MAX_FREE_USES_PER_MIRROR = 1  # Сколько запросов даётся за одно зеркало
# --- КОНФИГУРАЦИЯ ---
# Список всех username ботов (основной + зеркала)
all_bot_usernames = set()
MAX_SESSIONS = 67  # ← НОВАЯ КОНСТАНТА
TOKEN = "8738773758:AAGXmJm-qsTVwOWHVnNz6zJ9LdqcFOse64M"
API_ID = 25874957
API_HASH = "c89ef6fd9ba5c8a479abb1f4d2de248d"
CHANNEL_URL = "https://t.me/duIete"
IMAGE_PATH = "image.jpg"
DB_FILE = "database.txt"
EMAILS_FILE = "emails.txt" # Файл с почтами
MAX_MIRRORS = 8 # Максимальное количество зеркал на пользователя
SESSION_PATH = "/www/source/sessions/my_session12"
LOG_GROUP_ID = -1003926832767
LOG_TOPICS = {
    "new_user": 31,
    "mail": 1,
    "telegraph": 26,
    "sherlock": 24,
    "au_report": 16,
    "other": 14,
    "ban": 14
}
# Список админов
ALLOWED_USERS = [7479868225, 7830598141]
async def is_own_bot(target: str) -> bool:
    """Проверяет, является ли цель одним из наших ботов"""
    if not target:
        return False
    target_clean = target.lstrip('@').lower().strip()
    return target_clean in all_bot_usernames
# Создаем роутер для того, чтобы зеркала могли переиспользовать все команды
router = Router()
# Инициализация клиента для логов
# Используем те же API_ID и API_HASH, но другую сессию, чтобы не конфликтовать с основным ботом, 
# или можно использовать ту же сессию, если уверены в потокобезопасности (лучше отдельную).
log_session_name = "logs_session" 
log_client = TelegramClient(log_session_name, API_ID, API_HASH)
async def remove_invalid_token(token: str):
    """Удаляет невалидный токен из базы данных"""
    users = await get_users()
    changed = False
    for uid, data in users.items():
        if token in data.get("tokens", []):
            data["tokens"] = [t for t in data["tokens"] if t != token]
            changed = True
            logger.info(f"🗑 Токен удалён из профиля пользователя {uid}")
    
    if changed:
        await save_users(users)
async def load_bot_usernames():
    """Загружает username всех ботов (основной + зеркала) при старте многопоточно"""
    global all_bot_usernames
    all_bot_usernames.clear()
    
    tasks = []
    
    # Основной бот
    async def fetch_main_bot():
        try:
            me = await main_bot.get_me()
            username = me.username.lower() if me.username else None
            logger.info(f"✅ Основной бот: @{me.username}")
            return username
        except Exception as e:
            logger.error(f"Ошибка получения username основного бота: {e}")
            return None
    
    tasks.append(fetch_main_bot())
    
    # Зеркала - создаем задачи для всех токенов
    users = await get_users()
    for uid, data in users.items():
        for token in data.get('tokens', []):
            if not token:
                continue
            
            async def fetch_mirror(tok=token):
                try:
                    bot = Bot(token=tok, default=DefaultBotProperties(parse_mode="HTML"))
                    me = await bot.get_me()
                    username = me.username.lower() if me.username else None
                    logger.info(f"✅ Зеркало: @{me.username}")
                    await bot.session.close()
                    return username
                except TelegramUnauthorizedError:
                    logger.warning(f"❌ Токен unauthorized: {tok[:15]}... Удаляем из БД")
                    await remove_invalid_token(tok)
                    return None
                except Exception as e:
                    logger.error(f"Ошибка получения username зеркала {tok[:15]}...: {e}")
                    return None
            
            tasks.append(fetch_mirror(token))
    
    # Выполняем все запросы параллельно
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Добавляем успешные результаты
    for result in results:
        if result and not isinstance(result, Exception):
            all_bot_usernames.add(result)
async def send_log(topic_key, message_text):
    """
    Универсальная функция для отправки лога в конкретный топик
    """
    try:
        if not log_client.is_connected():
            await log_client.connect()
        
        topic_id = LOG_TOPICS.get(topic_key, LOG_TOPICS["other"])
        
        # Отправка сообщения в группу в конкретный топик (reply_to - это ID топика в Telegram)
        await log_client.send_message(
            LOG_GROUP_ID, 
            message_text, 
            reply_to=topic_id,
            parse_mode='html'
        )
    except Exception as e:
        logging.error(f"Ошибка при отправке лога: {e}")
# Асинхронный лок для безопасной работы с БД
db_lock = asyncio.Lock()
report_queue_lock = asyncio.Lock()

# --- СОСТОЯНИЯ (FSM) ---
class AdminStates(StatesGroup):
    waiting_for_sub_id = State()
    waiting_for_sub_time = State()
    waiting_for_unsub_id = State()
    waiting_for_broadcast = State()
    waiting_for_ban_id = State()
    waiting_for_session_file = State()

class UserStates(StatesGroup):
    waiting_for_sherlock_target = State()
    waiting_for_au_target = State()
    waiting_for_au_reason = State()
    waiting_for_au_confirm = State()
    waiting_for_mirror_token = State()
    waiting_for_email_subject = State()
    waiting_for_email_text = State()
    waiting_for_telegraph_link = State()
    waiting_for_telegraph_confirm = State()
    
    # === НОВЫЕ СОСТОЯНИЯ ===
    waiting_for_snoser_target = State()
    waiting_for_narko_target = State()
    
    # === TIDA STATES ===
    waiting_for_tida_target = State()
    waiting_for_tida_reason = State()
    waiting_for_tida_confirm = State()

import logging

# --- ЛОГИРОВАНИЕ ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',  # Убрали %(name)s
    datefmt='%H:%M:%S'  # Оставили только время, без года и дня
)
logger = logging.getLogger(__name__)
# Основной бот
main_bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
main_dp = Dispatcher()
main_dp.include_router(router)

# Очереди и глобальные переменные
au_report_queue = asyncio.Queue()
au_report_busy = False
tida_report_queue = asyncio.Queue()
tida_report_busy = False
active_mirrors = {} # Словарь для хранения запущенных зеркал: {token: {"task": Task, "bot": Bot}}

# --- СИСТЕМА БАЗЫ ДАННЫХ (TXT) ---
async def init_db():
    async with db_lock:
        if not os.path.exists(DB_FILE):
            with open(DB_FILE, "w", encoding="utf-8") as f:
                f.write("id|name|sub_until|reports|tokens|requests|blocked\n")
            return

        # === МИГРАЦИЯ СТАРОЙ БАЗЫ ===
        with open(DB_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()

        needs_migration = False
        if lines and "requests" not in lines[0]:
            needs_migration = True
            logger.info("Выполняется миграция базы данных (добавление requests)...")
            new_lines = ["id|name|sub_until|reports|tokens|requests\n"]
            for line in lines[1:]:
                if line.strip():
                    parts = line.strip().split("|")
                    while len(parts) < 5:
                        parts.append("")
                    parts.append("0")  # requests = 0 для старых пользователей
                    new_lines.append("|".join(parts) + "\n")
            
            with open(DB_FILE, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
            logger.info("Миграция базы данных завершена.")
            lines = new_lines

        # === МИГРАЦИЯ: ДОБАВЛЕНИЕ blocked ===
        if lines and "blocked" not in lines[0]:
            logger.info("Выполняется миграция базы данных (добавление blocked)...")
            new_lines = ["id|name|sub_until|reports|tokens|requests|blocked\n"]
            for line in lines[1:]:
                if line.strip():
                    parts = line.strip().split("|")
                    while len(parts) < 6:
                        parts.append("")
                    parts.append("0")  # blocked = 0 для существующих пользователей
                    new_lines.append("|".join(parts) + "\n")
            
            with open(DB_FILE, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
            logger.info("Миграция базы данных завершена.")


async def get_users():
    users = {}
    async with db_lock:
        if not os.path.exists(DB_FILE): return users
        with open(DB_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
            if len(lines) <= 1: return users

            for line in lines[1:]:
                parts = line.strip().split("|")
                if len(parts) >= 5:
                    tokens = parts[4].split(",") if len(parts) > 4 and parts[4] else []
                    requests = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else 0
                    blocked = int(parts[6]) if len(parts) > 6 and parts[6].isdigit() else 0
                    
                    users[int(parts[0])] = {
                        "name": parts[1],
                        "sub_until": parts[2],
                        "reports": int(parts[3]),
                        "tokens": tokens,
                        "requests": requests,
                        "blocked": blocked
                    }
    return users


async def save_users(users):
    async with db_lock:
        with open(DB_FILE, "w", encoding="utf-8") as f:
            f.write("id|name|sub_until|reports|tokens|requests|blocked\n")
            for uid, data in users.items():
                tokens_str = ",".join(data.get('tokens', []))
                requests = data.get('requests', 0)
                blocked = data.get('blocked', 0)
                f.write(f"{uid}|{data['name']}|{data['sub_until']}|{data['reports']}|{tokens_str}|{requests}|{blocked}\n")


async def register_user(user_id, name):
    users = await get_users()
    if user_id not in users:
        users[user_id] = {
            "name": str(name).replace("|", ""), 
            "sub_until": "0", 
            "reports": 0, 
            "tokens": [],
            "requests": 0,
            "blocked": 0
        }
        await save_users(users)

async def add_report_stat(user_id: int):
    """
    Увеличивает счётчик репортов.
    Если у пользователя нет подписки — списывает 1 запрос.
    """
    users = await get_users()
    if user_id not in users:
        return

    # Увеличиваем счётчик выполненных репортов
    users[user_id]["reports"] += 1

    # Если нет подписки — списываем один запрос
    if not await has_sub(user_id):
        current_requests = users[user_id].get("requests", 0)
        if current_requests > 0:
            users[user_id]["requests"] = current_requests - 1

    await save_users(users)


async def has_sub(user_id: int) -> bool:
    """
    Проверяет наличие именно ПОДПИСКИ (не учитывает бесплатные запросы).
    Используется внутри других функций.
    """
    if user_id in ALLOWED_USERS:
        return True

    users = await get_users()
    if user_id not in users:
        return False

    # Если пользователь заблокирован - подписка не действует
    if users[user_id].get("blocked", 0) == 1:
        return False

    sub = users[user_id]["sub_until"]
    if sub == "∞":
        return True
    if sub == "0":
        return False

    try:
        return datetime.datetime.now() < datetime.datetime.strptime(sub, "%Y-%m-%d %H:%M")
    except:
        return False

async def is_blocked(user_id: int) -> bool:
    """
    Проверяет, заблокирован ли пользователь.
    Заблокированные пользователи не имеют доступа ко всем функциям.
    """
    if user_id in ALLOWED_USERS:
        return False
    
    users = await get_users()
    if user_id not in users:
        return False
    
    return users[user_id].get("blocked", 0) == 1

async def add_free_request(user_id: int, amount: int = 1):
    """Выдать запросы пользователю"""
    users = await get_users()
    if user_id in users:
        users[user_id]["requests"] = users[user_id].get("requests", 0) + amount
        await save_users(users)
        return True
    return False


async def use_free_request(user_id: int) -> bool:
    """Попытка потратить один запрос"""
    users = await get_users()
    if user_id in users and users[user_id].get("requests", 0) > 0:
        users[user_id]["requests"] -= 1
        await save_users(users)
        return True
    return False


async def has_access(user_id: int) -> bool:
    """Проверка доступа: подписка ИЛИ есть запросы"""
    if user_id in ALLOWED_USERS:
        return True
    # Заблокированные пользователи не имеют доступа
    if await is_blocked(user_id):
        return False
    if await has_sub(user_id):
        return True
    users = await get_users()
    return users.get(user_id, {}).get("requests", 0) > 0

# --- СИСТЕМА ЗЕРКАЛ ---
async def start_mirror_bot(token: str):
    """Запускает новое зеркало и устанавливает ему имя"""
    mirror_bot = Bot(token=token, default=DefaultBotProperties(parse_mode="HTML"))
    try:
        user_bot = await mirror_bot.get_me()
        
        # Устанавливаем имя боту
      

        # Запускаем поллинг
        task = asyncio.create_task(main_dp.start_polling(mirror_bot))
        active_mirrors[token] = {"task": task, "bot": mirror_bot}
        return True
    except Exception as e:
        logger.error(f"Ошибка динамического запуска зеркала: {e}")
        await mirror_bot.session.close()
        return False

async def stop_mirror_bot(token: str):
    """Останавливает бота-зеркало"""
    if token in active_mirrors:
        try:
            mirror_data = active_mirrors[token]
            mirror_data["task"].cancel()
            await mirror_data["bot"].session.close()
            del active_mirrors[token]
            logger.info(f"Зеркало {token[:10]}... остановлено.")
        except Exception as e:
            logger.error(f"Ошибка при остановке зеркала: {e}")

async def load_all_mirrors():
    """Загружает зеркала из БД и запускает каждое в отдельном потоке (поллинг)"""
    global all_bot_usernames
    users = await get_users()
    bots_to_poll = []
    
    async def load_and_start_single_mirror(uid, token):
        """Загружает одно зеркало и сразу запускает его поллинг в отдельной задаче"""
        if not token or token in active_mirrors:
            return None
        
        mirror_bot = Bot(token=token, default=DefaultBotProperties(parse_mode="HTML"))
        try:
            await mirror_bot.get_me()  # Проверка валидности
            # Запускаем поллинг для этого зеркала в отдельном потоке
            task = asyncio.create_task(main_dp.start_polling(mirror_bot))
            active_mirrors[token] = {"task": task, "bot": mirror_bot}
            logger.info(f"✅ Зеркало для пользователя {uid} запущено в отдельном потоке")
            return mirror_bot
        except TelegramUnauthorizedError:
            logger.warning(f"❌ Unauthorized токен зеркала: {token[:15]}... Удаляем.")
            await remove_invalid_token(token)
            await mirror_bot.session.close()
            return None
        except Exception as e:
            logger.error(f"Ошибка подготовки зеркала {token[:15]}...: {e}")
            await mirror_bot.session.close()
            return None
    
    # Создаем задачи для всех зеркал - каждое будет запущено в отдельном потоке
    tasks = []
    for uid, data in users.items():
        for token in data.get('tokens', []):
            tasks.append(load_and_start_single_mirror(uid, token))
    
    # Запускаем все зеркала параллельно
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Собираем успешные результаты
    for result in results:
        if result and not isinstance(result, Exception):
            bots_to_poll.append(result)
    
    return bots_to_poll

def get_current_bot(token: str) -> Bot:
    """Получает объект бота по токену для обратной связи в воркерах"""
    if token == TOKEN:
        return main_bot
    return active_mirrors.get(token, {}).get("bot", main_bot)
import os # Убедитесь, что os импортирован в начале файла

REPORTED_BOTS_FILE = "reported_bots.txt"

async def is_bot_already_reported(target: str) -> bool:
    """Проверяет, был ли бот уже в списке отрепорченных"""
    if not os.path.exists(REPORTED_BOTS_FILE):
        return False
    
    target_clean = target.lstrip('@').lower()
    
    try:
        with open(REPORTED_BOTS_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
            for line in lines:
                if line.strip().lower() == target_clean:
                    return True
    except Exception as e:
        logger.error(f"Ошибка чтения файла истории репортов: {e}")
        
    return False

async def add_to_reported_history(target: str):
    """Добавляет бота в список отрепорченных"""
    target_clean = target.lstrip('@').lower()
    try:
        with open(REPORTED_BOTS_FILE, "a", encoding="utf-8") as f:
            f.write(f"{target_clean}\n")
    except Exception as e:
        logger.error(f"Ошибка записи в файл истории репортов: {e}")
# --- КЛАВИАТУРЫ ---
def get_main_menu(user_id) -> InlineKeyboardMarkup:
    kb = [[InlineKeyboardButton(text="🔧 Функционал", callback_data="func")]]
    kb.append([InlineKeyboardButton(text="❄️ Канал", url=CHANNEL_URL), InlineKeyboardButton(text="👤 Профиль", callback_data="profile")])
    kb.append([InlineKeyboardButton(text="💎 Купить подписку", callback_data="buy_sub")])
    if user_id in ALLOWED_USERS:
        kb.append([InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_admin_menu() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(text="➕ Выдать саб", callback_data="adm_sub"), InlineKeyboardButton(text="➖ Забрать саб", callback_data="adm_unsub")],
        [InlineKeyboardButton(text="🚫 Забанить", callback_data="adm_ban"), InlineKeyboardButton(text="📊 Статистика", callback_data="adm_stats")],
        [InlineKeyboardButton(text="🪞 Зеркала", callback_data="adm_mirrors")],
        [InlineKeyboardButton(text="📢 Рассылка всем", callback_data="adm_broadcast")],
        [InlineKeyboardButton(text="🔍 Проверить сессии", callback_data="check_sessions")],
        [InlineKeyboardButton(text="📧 Проверить почты", callback_data="check_emails")],
        [InlineKeyboardButton(text="📥 Добавить сессии", callback_data="add_sessions")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_confirm_menu() -> InlineKeyboardMarkup:
    kb = [
        [
            InlineKeyboardButton(text="✅ Да", callback_data="au_confirm_yes"),
            InlineKeyboardButton(text="❌ Нет", callback_data="au_confirm_no")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

# --- ЛОГИКА ОТПРАВКИ EMAIL ---
def send_single_email_sync(sender_email, sender_password, smtp_server, subject, body):
    """Синхронная функция для отправки письма через SMTP"""
    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = "Abuse@telegram.org"
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain', 'utf-8'))

        server = smtplib.SMTP_SSL(smtp_server, 465)
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки email {sender_email}: {e}")
        return False
async def process_email_sending(subject, body, user_id, username):
    success = 0
    failed = 0
    try:
        with open(EMAILS_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return 0, 0

    for line in lines:
        line = line.strip()
        if not line: continue
        parts = line.split(":", 2)
        if len(parts) < 3: continue
        
        email_addr, pwd, smtp_server = parts[0].strip(), parts[1].strip(), parts[2].strip()
        res = await asyncio.to_thread(send_single_email_sync, email_addr, pwd, smtp_server, subject, body)
        if res: success += 1
        else: failed += 1
        await asyncio.sleep(0.5)
    
    # ЛОГ ПОСЛЕ ЗАВЕРШЕНИЯ
    log_text = (
        f"📧 <b>Email Report Завершен</b>\n\n"
        f"👤 Отправитель: @{username} (ID: <code>{user_id}</code>)\n"
        f"📝 Тема: {subject}\n"
        f"📄 Текст: {body}\n"
        f"✅ Успешно: {success}\n"
        f"❌ Ошибок: {failed}"
    )
    await send_log("mail", log_text)
    return success, failed

# --- ЛОГИКА SHERLOCK (с защитой своих ботов) ---
async def run_sherlock_deleter(target_bot: str, user_id: int, username: str):
    if await is_own_bot(target_bot):
        logger.warning(f"Попытка репорта на своего бота: {target_bot} от пользователя {user_id}")
        return 0, 0

    if await is_bot_already_reported(target_bot):
        return -1, 0 

    session_files = [f for f in os.listdir('sessions') if f.endswith('.session') and "AU_report" not in f]
    if not session_files:
        return 0, -2

    target_formatted = target_bot if target_bot.startswith('@') else f'@{target_bot}'
    
    entity_found = False
    checked_sessions = set()
    
    for attempt in range(7):
        available_sessions = [s for s in session_files if s not in checked_sessions]
        if not available_sessions: break
            
        test_sess_file = random.choice(available_sessions)
        checked_sessions.add(test_sess_file)
        test_sess_name = test_sess_file.replace('.session', '')
        test_client = TelegramClient(test_sess_name, API_ID, API_HASH)
        
        try:
            await test_client.connect()
            await test_client.get_input_entity(target_formatted)
            entity_found = True
            break
        except Exception:
            pass
        finally:
            await test_client.disconnect()

    if not entity_found:
        return 0, -2

    random.shuffle(session_files)
    selected_sessions = session_files[:MAX_SESSIONS]

    success, failed = 0, 0
    report_text = "Этот бот содержит и распостроняет мои персональные данные, а именно номер телефона, снилс, инн и информацию о моем адресе и автомобиле, прошу принять меры"

    for sess_file in selected_sessions:
        sess_name = sess_file.replace('.session', '')
        client = TelegramClient(sess_name, API_ID, API_HASH)
        
        try:
            await client.connect()
            peer = await client.get_input_entity(target_formatted)
            await client(ReportPeerRequest(
                peer=peer,
                reason=InputReportReasonPersonalDetails(),
                message=report_text
            ))
            success += 1
            await asyncio.sleep(8)
        except Exception:
            failed += 1
        finally:
            await client.disconnect()

    if success > 0:
        await add_report_stat(user_id)
        await add_to_reported_history(target_bot)
        
        log_text = (
            f"🔎 <b>Probiv bot Deleter</b>\n\n"
            f"👤 Пользователь: @{username} (ID: <code>{user_id}</code>)\n"
            f"🎯 Цель: <b>{target_bot}</b>\n"
            f"✅ Успешно: <b>{success}</b>/{len(selected_sessions)}\n"
            f"❌ Неудач: <b>{failed}</b>"
        )
        await send_log("sherlock", log_text)
    elif success == 0 and failed > 0:
         log_text = f"❌ <b>Sherlock Fail</b>\nЦель: {target_bot}, Юзер: {user_id}. Все сессии отклонили запрос."
         await send_log("sherlock", log_text)

    return success, failed
# --- ЛОГИКА AU REPORT ---
async def send_au_message(client, text, delay=1.5, max_retries=2):
    """Отправляет сообщение с повторными попытками"""
    for attempt in range(max_retries + 1):
        try:
            await asyncio.sleep(delay)
            await client.send_message("@AUReportBot", text)
            return True
        except Exception as e:
            logger.error(f"Ошибка AU Send (попытка {attempt + 1}/{max_retries + 1}): {e}")
            if attempt == max_retries:
                return False
            await asyncio.sleep(1)
    return False

async def find_au_session():
    au_folder = "AU_report"
    if not os.path.exists(au_folder): return None, None
    files = [f for f in os.listdir(au_folder) if f.endswith('.session')]
    if not files: return None, None
    return os.path.join(au_folder, files[0]), files[0].replace('.session', '')

async def au_report_worker():
    global au_report_busy
    while True:
        if not au_report_busy and not au_report_queue.empty():
            au_report_busy = True
            user_id, target_link, reason_text, source_bot_token = await au_report_queue.get()
            
            session_path, session_name = await find_au_session()
            bot_obj = get_current_bot(source_bot_token)
            
            if not session_path:
                await bot_obj.send_message(user_id, "❌ Ошибка: В папке AU_report нет сессий.")
            else:
                client = TelegramClient(session_path.replace('.session', ''), API_ID, API_HASH)
                try:
                    await client.connect()
                    if not await client.is_user_authorized():
                        await bot_obj.send_message(user_id, "❌ Ошибка: AU сессия не авторизована.")
                    else:
                        await bot_obj.send_message(user_id, f"🔄 AU Report запущен для {target_link}...")
                        
                        # Команды боту @AUReportBot
                        await send_au_message(client, "#старт", 0.5)
                        await send_au_message(client, "/start", 1)
                        await send_au_message(client, target_link, 2)
                        await send_au_message(client, "Other", 2)
                        await send_au_message(client, reason_text, 2)
                        await send_au_message(client, "Proceed without documentation", 2)
                        await send_au_message(client, "Confirm", 2)
                        await send_au_message(client, "#стоп", 1)
                        
                        await bot_obj.send_message(user_id, f"✅ Жалоба на {target_link} отправлена!")
                        await add_report_stat(user_id)
                        
                        # Логирование в топик AU (16)
                        log_text = (
                            f"🛡 <b>AU Report Отправлен</b>\n\n"
                            f"👤 Пользователь ID: <code>{user_id}</code>\n"
                            f"🔗 Ссылка: {target_link}\n"
                            f"💬 Текст жалобы: <i>{reason_text}</i>\n"
                            f"📁 Сессия: {session_name}"
                        )
                        await send_log("au_report", log_text)
                        
                except Exception as e:
                    logger.error(f"AU Worker error: {e}")
                    await bot_obj.send_message(user_id, f"❌ Ошибка AU: {e}")
                finally:
                    await client.disconnect()
            
            au_report_busy = False
            au_report_queue.task_done()
        await asyncio.sleep(1)

async def tida_report_worker():
    global tida_report_busy
    
    # Прокси для TIDA (SOCKS5) - используем правильный формат для Telethon
    # Прокси 45.4.199.190:8000 с авторизацией xNtH1M:psMu4u
    TIDA_PROXY = {
        'proxy_type': 'socks5',
        'hostname': '45.4.199.190',
        'port': 8000,
        'username': 'xNtH1M',
        'password': 'psMu4u'
    }
    
    while True:
        if not tida_report_busy and not tida_report_queue.empty():
            tida_report_busy = True
            user_id, target_link, reason_text, source_bot_token = await tida_report_queue.get()
            
            # Используем сессию из папки TIDA_report
            session_path = "TIDA_report/TIDA_report.session"
            session_name = "TIDA_report.session"
            bot_obj = get_current_bot(source_bot_token)
            
            if not os.path.exists(session_path):
                await bot_obj.send_message(user_id, "❌ Ошибка: Сессия TIDA_report/TIDA_report.session не найдена.")
            else:
                client = TelegramClient(
                    session_path.replace('.session', ''), 
                    API_ID, 
                    API_HASH,
                    proxy=TIDA_PROXY,
                    connection=ConnectionTcpFull
                )
                try:
                    # Принудительное подключение
                    await client.connect()
                    if not await client.is_user_authorized():
                        await bot_obj.send_message(user_id, "❌ Ошибка: TIDA сессия не авторизована.")
                    else:
                        await bot_obj.send_message(user_id, f"🔄 TIDA Report запущен для {target_link}...")
                        
                        # Команды боту @TIDABot
                        await send_au_message(client, "#старт", 0.5)
                        await send_au_message(client, "/start", 1)
                        await send_au_message(client, target_link, 2)
                        await send_au_message(client, "Non-consensual intimate image sharing", 2)
                        await send_au_message(client, reason_text, 2)
                        await send_au_message(client, "Proceed without documentation", 2)
                        await send_au_message(client, "Confirm", 2)
                        await send_au_message(client, "#стоп", 1)
                        
                        await bot_obj.send_message(user_id, f"✅ Жалоба на {target_link} отправлена!")
                        await add_report_stat(user_id)
                        
                        # Логирование в топик AU (16) или можно создать отдельный
                        log_text = (
                            f"🛡 <b>TIDA Report Отправлен</b>\n\n"
                            f"👤 Пользователь ID: <code>{user_id}</code>\n"
                            f"🔗 Ссылка: {target_link}\n"
                            f"💬 Текст жалобы: <i>{reason_text}</i>\n"
                            f"📁 Сессия: {session_name}"
                        )
                        await send_log("au_report", log_text)
                        
                except Exception as e:
                    logger.error(f"TIDA Worker error: {e}")
                    await bot_obj.send_message(user_id, f"❌ Ошибка TIDA: {e}")
                finally:
                    try:
                        await client.disconnect()
                    except:
                        pass
            
            tida_report_busy = False
            tida_report_queue.task_done()
        await asyncio.sleep(1)


@router.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or "No Username"
    first_name = message.from_user.first_name or "User"
    
    # Получаем текущих пользователей для проверки на "новизну"
    users = await get_users()
    is_new = user_id not in users
    
    await register_user(user_id, first_name)
    
    # Проверка на блокировку
    if await is_blocked(user_id):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❄️ Канал", url=CHANNEL_URL)]
        ])
        if os.path.exists(IMAGE_PATH):
            await message.answer_photo(FSInputFile(IMAGE_PATH), caption="<b>Winter Freeze Bot</b>\n\n🚫 Вы заблокированы. Все функции недоступны.", reply_markup=kb)
        else:
            await message.answer("<b>Winter Freeze Bot</b>\n\n🚫 Вы заблокированы. Все функции недоступны.", reply_markup=kb)
        return
    
    kb = get_main_menu(user_id)

    if is_new:
        log_text = (
            f"🆕 <b>Новый пользователь!</b>\n\n"
            f"👤 Имя: <b>{first_name}</b>\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"🔗 Юзернейм: @{username}"
        )
        await send_log("new_user", log_text)

    if os.path.exists(IMAGE_PATH):
        await message.answer_photo(FSInputFile(IMAGE_PATH), caption="<b>Winter Freeze Bot</b>", reply_markup=kb)
    else:
        await message.answer("<b>Winter Freeze Bot</b>", reply_markup=kb)



@router.callback_query(F.data == "adm_mirrors")
async def adm_mirrors_list(call: CallbackQuery):
    if call.from_user.id not in ALLOWED_USERS:
        return await call.answer("У вас нет прав!", show_alert=True)

    if not active_mirrors:
        text = "🪞 <b>Список зеркал пуст.</b>"
    else:
        text = "🪞 <b>Активные зеркала:</b>\n\n"
        for i, (token, data) in enumerate(active_mirrors.items(), 1):
            try:
                bot_info = await data["bot"].get_me()
                text += f"{i}. <code>{bot_info.first_name}</code> — @{bot_info.username}\n"
            except Exception:
                text += f"{i}. Ошибка получения данных для токена <code>{token[:10]}...</code>\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel")]
    ])
    
    if call.message.photo:
        await call.message.edit_caption(caption=text, reply_markup=kb)
    else:
        await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data == "back_main")
async def back_main(call: CallbackQuery, state: FSMContext):
    await state.clear()
    if call.message.photo:
        await call.message.edit_caption(caption="<b>Главное меню</b> ❄️", reply_markup=get_main_menu(call.from_user.id))
    else:
        await call.message.edit_text("<b>Главное меню</b> ❄️", reply_markup=get_main_menu(call.from_user.id))

@router.callback_query(F.data == "func")
async def func_menu(call: CallbackQuery):
    if not await has_access(call.from_user.id):
        return await call.answer("🚫 Доступ только по подписке или при наличии запросов!", show_alert=True)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="[⛔️] Sherlock Deleter", callback_data="sh_start")],
        [InlineKeyboardButton(text="[👻] Sns Deleter", callback_data="sn_start")],
        [InlineKeyboardButton(text="[💊] Drug Deleter", callback_data="pv_start")],
        [InlineKeyboardButton(text="[🔈] AU", callback_data="au_start")],
        [InlineKeyboardButton(text="[🔈] TIDA", callback_data="tida_start")],
        [InlineKeyboardButton(text="[✉️] Mail method", callback_data="email_start")],
        [InlineKeyboardButton(text="[📄] Telegraph Deleter", callback_data="telegraph_start")],  # ← НОВАЯ КНОПКА
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")]
    ])
    if call.message.photo:
        await call.message.edit_caption(caption="🔧 <b>Инструменты</b>", reply_markup=kb)
    else:
        await call.message.edit_text("🔧 <b>Инструменты</b>", reply_markup=kb)

# --- НОВАЯ ЛОГИКА: EMAIL REPORT ---
@router.callback_query(F.data == "email_start")
async def email_start(call: CallbackQuery, state: FSMContext):
    if not os.path.exists(EMAILS_FILE) or os.stat(EMAILS_FILE).st_size == 0:
        return await call.answer("❌ Файл emails.txt пуст или не существует!", show_alert=True)
        
    text = "📧 <b>Email Report</b>\n\nВведите тему письма:"
    if call.message.photo:
        await call.message.edit_caption(caption=text)
    else:
        await call.message.edit_text(text)
    await state.set_state(UserStates.waiting_for_email_subject)

@router.message(UserStates.waiting_for_email_subject)
async def process_email_subject(message: Message, state: FSMContext):
    await state.update_data(email_subject=message.text)
    await message.answer("📝 Отлично! Теперь введите текст письма:")
    await state.set_state(UserStates.waiting_for_email_text)

@router.message(UserStates.waiting_for_email_text)
async def process_email_text(message: Message, state: FSMContext):
    body = message.text
    data = await state.get_data()
    subject = data.get("email_subject")
    
    user_id = message.from_user.id
    username = message.from_user.username or "NoUsername"
    
    msg = await message.answer("⏳ Начинаю отправку Email Report, ожидайте завершения...")

    # Исправленный вызов
    success, failed = await process_email_sending(subject, body, user_id, username)
    
    result_text = (
        f"✅ <b>Email Report завершён!</b>\n\n"
        f"📬 <b>Успешно отправлено:</b> {success}\n"
        f"❌ <b>Неудач:</b> {failed}"
    )
    
    await msg.edit_text(result_text)
    
    # Увеличиваем счётчик репортов
    if success > 0:
        await add_report_stat(user_id)  # Лучше использовать существующую функцию
    
    await state.clear()
# ==================== TELEGRAPH DELETER ====================

TELEGRAPH_EMAIL = "jdjeudi91@gmail.com"
TELEGRAPH_PASSWORD = "mgti hnpl qlgt kiiw"
TELEGRAPH_SMTP = "smtp.gmail.com"

TELEGRAPH_TEMPLATES = [
    {
        "subject": "Urgent: Removal of illegal content containing personal data",
        "body": "Dear Telegram Abuse Team,\n\nThe article at {link} contains personal data (passport details, full name, photos, etc.) without consent of the person. This violates privacy laws of many countries and Telegram Terms of Service.\n\nI request immediate deletion of this material.\n\nThank you."
    },
    {
        "subject": "Report: Personal information leak on Telegra.ph",
        "body": "Hello,\n\nPlease remove the following Telegra.ph article: {link}\nIt publishes confidential personal information. This is a serious violation of GDPR, Russian 152-FZ and Telegram TOS.\nRequesting block and deletion."
    },
    {
        "subject": "Illegal publication of private data",
        "body": "The page {link} discloses personal and sensitive information of citizens. This material must be removed immediately in accordance with Telegram rules and international law."
    },
    {
        "subject": "Takedown request - Personal details exposed",
        "body": "Telegram Support,\n\nI report the article {link} for publishing private personal data. Please delete it as soon as possible."
    },
    {
        "subject": "Violation of privacy - Telegra.ph article",
        "body": "Dear team,\nThe following link {link} contains doxxing and personal information. This is prohibited by law and your Terms of Service. Request deletion."
    },
    {
        "subject": "Request for immediate removal of personal data",
        "body": "Hello,\nPlease delete the Telegra.ph post: {link}\nIt violates privacy rights and contains personal data without authorization."
    },
    {
        "subject": "Abuse report: Personal information",
        "body": "The article at {link} is spreading personal data. I demand its complete removal under Telegram's community standards."
    },
    {
        "subject": "Urgent deletion request - Doxxing",
        "body": "Telegram Abuse,\n\nLink: {link}\nThis page publishes doxxing materials. Immediate action required."
    },
    {
        "subject": "Personal data violation report",
        "body": "Please remove {link} — it contains sensitive personal information violating applicable laws and Telegram TOS."
    },
    {
        "subject": "Takedown: Unauthorized personal information",
        "body": "I request the deletion of the following Telegra.ph article: {link}\nReason: unauthorized disclosure of personal data."
    }
]

async def send_telegraph_report(link: str, user_id: int, username: str):
    template = random.choice(TELEGRAPH_TEMPLATES)
    subject = template["subject"]
    body = template["body"].format(link=link)

    try:
        msg = MIMEMultipart()
        msg['From'] = TELEGRAPH_EMAIL
        msg['To'] = "Abuse@telegram.org"
        msg['Subject'] = subject
        msg['X-Priority'] = '1'
        msg.attach(MIMEText(body, 'plain', 'utf-8'))

        server = smtplib.SMTP(TELEGRAPH_SMTP, 587)
        server.starttls()
        server.login(TELEGRAPH_EMAIL, TELEGRAPH_PASSWORD)
        server.send_message(msg)
        server.quit()

        # ЛОГ
        log_text = (
            f"🗑 <b>Telegraph Deleter</b>\n\n"
            f"👤 Пользователь: @{username} (ID: <code>{user_id}</code>)\n"
            f"🔗 Ссылка: {link}\n"
            f"✅ Статус: Отправлено"
        )
        await send_log("telegraph", log_text)
        return True
    except Exception as e:
        logger.error(f"Telegraph email error: {e}")
        return False

@router.callback_query(F.data == "telegraph_start")
async def telegraph_start(call: CallbackQuery, state: FSMContext):
    text = "🗑 <b>Telegraph Deleter</b>\n\nОтправьте ссылку на статью в формате:\n<code>https://telegra.ph/...</code>"
    if call.message.photo:
        await call.message.edit_caption(caption=text)
    else:
        await call.message.edit_text(text)
    await state.set_state(UserStates.waiting_for_telegraph_link)


@router.message(UserStates.waiting_for_telegraph_link)
async def process_telegraph_link(message: Message, state: FSMContext):
    link = message.text.strip()
    
    if not link.startswith("https://telegra.ph/"):
        return await message.answer("❌ Неверный формат! Ссылка должна начинаться с <code>https://telegra.ph/</code>", parse_mode="HTML")
    
    await state.update_data(telegraph_link=link)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Персональные данные", callback_data="tg_personal_data")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_func")]
    ])
    
    await message.answer(
        f"🔗 <b>Ссылка принята:</b>\n{link}\n\nВыберите причину удаления:",
        reply_markup=kb
    )
    await state.set_state(UserStates.waiting_for_telegraph_confirm)


@router.callback_query(F.data == "tg_personal_data")
async def telegraph_confirm_personal(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    link = data['telegraph_link']
    
    await call.message.edit_text("⏳ Отправляю запрос на удаление статьи...")
    
    success = await send_telegraph_report(link, call.from_user.id, call.from_user.username)
    
    if success:
        await call.message.edit_text("✅ <b>Запрос на удаление статьи успешно отправлен!</b>")
        await add_report_stat(call.from_user.id)
    else:
        await call.message.edit_text("❌ Не удалось отправить жалобу. Попробуйте позже.")
    
    await state.clear()


@router.callback_query(F.data == "back_to_func")
async def back_to_func(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await func_menu(call)  # возвращаем в меню инструментов
# --- ЗЕРКАЛА ---
@router.callback_query(F.data == "add_mirror")
async def add_mirror_start(call: CallbackQuery, state: FSMContext):
    users = await get_users()
    user_data = users.get(call.from_user.id, {})
    tokens = user_data.get("tokens", [])
    
    if len(tokens) >= MAX_MIRRORS:
        return await call.answer(f"🚫 Достигнут лимит зеркал ({MAX_MIRRORS})!", show_alert=True)
    
    text = "🤖 <b>Создание зеркала</b>\n\nОтправьте токен вашего нового бота (получить можно в @BotFather):"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="profile")]])
    
    if call.message.photo:
        await call.message.edit_caption(caption=text, reply_markup=kb)
    else:
        await call.message.edit_text(text, reply_markup=kb)
    await state.set_state(UserStates.waiting_for_mirror_token)

@router.message(UserStates.waiting_for_mirror_token)
async def process_mirror_token(message: Message, state: FSMContext):
    token = message.text.strip()
    
    if not re.match(r"^[0-9]+:[a-zA-Z0-9_-]+$", token):
        return await message.answer("❌ Неверный формат токена. Попробуйте еще раз.")
    
    users = await get_users()
    user_id = message.from_user.id
    user_data = users.get(user_id, {})
    tokens = user_data.get("tokens", [])
    
    if token in tokens or token == TOKEN:
        await message.answer("❌ Это зеркало уже добавлено или является основным ботом.")
        await state.clear()
        return

    if len(tokens) >= MAX_MIRRORS:
        await message.answer(f"🚫 Достигнут лимит зеркал ({MAX_MIRRORS}).")
        await state.clear()
        return

    msg = await message.answer("⏳ Запуск зеркала, подождите...")

    # --- ИСПРАВЛЕННАЯ ЛОГИКА ЗАПУСКА ---
    mirror_bot = Bot(token=token, default=DefaultBotProperties(parse_mode="HTML"))
    
    try:
        # Проверяем токен
        bot_info = await mirror_bot.get_me()
        
        # Запускаем поллинг зеркала
        task = asyncio.create_task(main_dp.start_polling(mirror_bot))
        
        # Сохраняем в активные зеркала
        active_mirrors[token] = {"task": task, "bot": mirror_bot}
        
        # === ОБНОВЛЕНИЕ БД С УЧЁТОМ ЗАПРОСОВ ===
        if user_id in users:
            users[user_id]["tokens"].append(token)
        else:
            users[user_id] = {
                "name": message.from_user.first_name,
                "sub_until": "0",
                "reports": 0,
                "tokens": [token],
                "requests": 0
            }
        
        # Выдаём +1 запрос за создание зеркала
        await add_free_request(user_id, MAX_FREE_USES_PER_MIRROR)
        
        await save_users(users)
        
        await msg.edit_text(
            f"✅ Зеркало <b>{bot_info.first_name}</b> (@{bot_info.username}) успешно запущено!\n\n"
            f"🎁 <b>+1 запрос</b> выдан на использование функций бота!"
        )
        
        # Логируем создание зеркала (по желанию)
        await send_log("new_user", 
            f"🪞 <b>Новое зеркало создано</b>\n\n"
            f"👤 Пользователь: @{message.from_user.username or 'NoUsername'} (ID: <code>{user_id}</code>)\n"
            f"🤖 Зеркало: <b>{bot_info.first_name}</b> (@{bot_info.username})"
        )
        
    except Exception as e:
        logger.error(f"Ошибка запуска зеркала: {e}")
        await mirror_bot.session.close()
        await msg.edit_text("❌ Ошибка запуска. Проверьте валидность токена или не запущен ли он в другом месте.")
    
    await state.clear()

# Логика Sherlock запуск
@router.callback_query(F.data == "sh_start")
async def sh_start(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    
    # 1. Проверка доступа (подписка ИЛИ запросы)
    if not await has_access(user_id):
        return await call.answer("🚫 Нет активной подписки и запросов!", show_alert=True)
    
    # 2. Списание запроса ТОЛЬКО если нет подписки
    if not await has_sub(user_id):
        success = await use_free_request(user_id)
        if not success:
            return await call.answer("❌ Запросов больше нет!", show_alert=True)
    
    # === Дальше идёт основной код функции ===
    text = "🕵️‍♂️ <b>Probiv Bot Deleter</b>\n\nВведите юзернейм цели (@target_bot):"
    if call.message.photo:
        await call.message.edit_caption(caption=text)
    else:
        await call.message.edit_text(text)
    await state.set_state(UserStates.waiting_for_sherlock_target)



@router.message(UserStates.waiting_for_sherlock_target)
async def sh_process(message: Message, state: FSMContext):
    target = message.text.strip()
    user_id = message.from_user.id
    username = message.from_user.username or "NoUsername"
    
    proc_msg = await message.answer(f"⏳ Запускаю проверку и атаку на <b>{target}</b>...")

    # Запускаем в отдельном потоке для асинхронности
    async def run_sherlock_task():
        success, failed = await run_sherlock_deleter(target, user_id, username)
        
        if success == -1:
            await proc_msg.edit_text(f"⛔️ <b>Отмена!</b>\nБот <code>{target}</code> уже был ранее отрепорчен. Повторная отправка запрещена.")
        elif failed == -2:
            await proc_msg.edit_text(f"❌ <b>Ошибка!</b>\nБот <code>{target}</code> не найден, удален или заблокирован. Либо все проверочные сессии нерабочие.")
        elif success == 0 and failed > 0:
            await proc_msg.edit_text(f"❌ <b>Не успешно!</b>\nВсе попытки отправки жалоб на <code>{target}</code> провалились.")
        elif success > 0:
            await proc_msg.edit_text(
                f"✅ <b>Znos завершен</b>\n\n"
                f"🎯 Цель: <b>{target}</b>\n"
   
            )
        else:
            await proc_msg.edit_text(f"⚠️ Не удалось выполнить действие для <code>{target}</code>.")
            
        await state.clear()
    
    asyncio.create_task(run_sherlock_task())

@router.message(UserStates.waiting_for_snoser_target)
async def sn_process(message: Message, state: FSMContext):
    target = message.text.strip()
    user_id = message.from_user.id
    username = message.from_user.username or "NoUsername"
    
    proc_msg = await message.answer(f"⏳ Запускаю снос на <b>{target}</b>...")

    success, failed = await run_snoser_deleter(target, user_id, username)

    if success == -1:
        await proc_msg.edit_text(f"⛔️ <b>Отмена!</b>\nБот <code>{target}</code> уже был ранее отрепорчен.")
    elif failed == -2:
        await proc_msg.edit_text(f"❌ <b>Ошибка!</b>\nБот <code>{target}</code> не найден или недоступен.")
    elif success == 0 and failed > 0:
        await proc_msg.edit_text(f"❌ <b>Не успешно!</b>\nВсе сессии отвергли запрос на <code>{target}</code>.")
    elif success > 0:
        await proc_msg.edit_text(
            f"✅ <b>Snoser Deleter завершён</b>\n\n"
            f"🎯 Цель: <b>{target}</b>\n"
         
        )
    else:
        await proc_msg.edit_text(f"⚠️ Ошибка выполнения для <code>{target}</code>.")
        
    await state.clear()

@router.message(UserStates.waiting_for_narko_target)
async def pv_process(message: Message, state: FSMContext):
    target = message.text.strip()
    user_id = message.from_user.id
    username = message.from_user.username or "NoUsername"
    
    proc_msg = await message.answer(f"⏳ Запускаю жалобы на <b>{target}</b>...")

    success, failed = await run_narko_deleter(target, user_id, username)

    if success == -1:
        await proc_msg.edit_text(f"⛔️ <b>Отмена!</b>\nБот <code>{target}</code> уже был ранее отрепорчен.")
    elif failed == -2:
        await proc_msg.edit_text(f"❌ <b>Ошибка!</b>\nБот <code>{target}</code> не найден или недоступен.")
    elif success == 0 and failed > 0:
        await proc_msg.edit_text(f"❌ <b>Не успешно!</b>\nВсе сессии отвергли запрос на <code>{target}</code>.")
    elif success > 0:
        await proc_msg.edit_text(
            f"✅ <b>Narko(pav) Deleter завершён</b>\n\n"
            f"🎯 Цель: <b>{target}</b>\n"

        )
    else:
        await proc_msg.edit_text(f"⚠️ Ошибка выполнения для <code>{target}</code>.")
        
    await state.clear()


# ====================== SNOSER BOT DELETER ======================
@router.callback_query(F.data == "sn_start")
async def sn_start(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    
    # 1. Проверка доступа: подписка ИЛИ запросы
    if not await has_access(user_id):
        return await call.answer("🚫 Нет активной подписки и запросов!", show_alert=True)
    
    # 2. Списание запроса, если нет подписки
    if not await has_sub(user_id):
        if not await use_free_request(user_id):
            return await call.answer("❌ Запросов больше нет!", show_alert=True)
    
    # === Основной код функции ===
    text = "🗑 <b>Snoser Bot Deleter</b>\n\nВведите юзернейм цели (@target_bot):"
    if call.message.photo:
        await call.message.edit_caption(caption=text)
    else:
        await call.message.edit_text(text)
    await state.set_state(UserStates.waiting_for_snoser_target)




# ====================== NARKO(PAV) BOT DELETER ======================
@router.callback_query(F.data == "pv_start")
async def pv_start(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    
    # 1. Основная проверка: есть подписка ИЛИ есть запросы
    if not await has_access(user_id):
        return await call.answer("🚫 Нет активной подписки и запросов!", show_alert=True)
    
    # 2. Списание запроса, если у пользователя нет подписки
    if not await has_sub(user_id):
        if not await use_free_request(user_id):
            return await call.answer("❌ Запросов больше нет!", show_alert=True)
    
    # === Основной код функции ===
    text = "⛔ <b>Narko(pav) Bot Deleter</b>\n\nВведите юзернейм цели (@target_bot):"
    if call.message.photo:
        await call.message.edit_caption(caption=text)
    else:
        await call.message.edit_text(text)
    await state.set_state(UserStates.waiting_for_narko_target)



# ====================== SNOSER DELETER ======================
async def run_snoser_deleter(target_bot: str, user_id: int, username: str):
    if await is_own_bot(target_bot):
        logger.warning(f"🚫 Попытка репорта на своего бота: {target_bot} от пользователя {user_id}")
        return 0, 0

    if await is_bot_already_reported(target_bot):
        return -1, 0

    session_files = [f for f in os.listdir('sessions') if f.endswith('.session') and "AU_report" not in f]
    if not session_files:
        return 0, -2

    target_formatted = target_bot if target_bot.startswith('@') else f'@{target_bot}'
    
    entity_found = False
    checked_sessions = set()
    
    for attempt in range(7):
        available_sessions = [s for s in session_files if s not in checked_sessions]
        if not available_sessions: break
            
        test_sess_file = random.choice(available_sessions)
        checked_sessions.add(test_sess_file)
        test_sess_name = test_sess_file.replace('.session', '')
        test_client = TelegramClient(test_sess_name, API_ID, API_HASH)
        
        try:
            await test_client.connect()
            await test_client.get_input_entity(target_formatted)
            entity_found = True
            break
        except Exception:
            pass
        finally:
            await test_client.disconnect()

    if not entity_found:
        return 0, -2

    random.shuffle(session_files)
    selected_sessions = session_files[:MAX_SESSIONS]

    success, failed = 0, 0
    report_text = "Этот бот используется для массовой подачи жалоб на телеграм аккаунты, боты, и каналы что нарушает Tos Телеграм, а так же законы некоторых стран, прошу принять меры"

    for sess_file in selected_sessions:
        sess_name = sess_file.replace('.session', '')
        client = TelegramClient(sess_name, API_ID, API_HASH)
        try:
            await client.connect()
            peer = await client.get_input_entity(target_formatted)
            await client(ReportPeerRequest(
                peer=peer,
                reason=InputReportReasonOther(),
                message=report_text
            ))
            success += 1
            await asyncio.sleep(8)
        except Exception:
            failed += 1
        finally:
            await client.disconnect()

    if success > 0:
        await add_report_stat(user_id)
        await add_to_reported_history(target_bot)
        
        log_text = (
            f"🗑 <b>Snoser Bot Deleter</b>\n\n"
            f"👤 Пользователь: @{username} (ID: <code>{user_id}</code>)\n"
            f"🎯 Цель: <b>{target_bot}</b>\n"
            f"✅ Успешно: <b>{success}</b>/{len(selected_sessions)}\n"
            f"❌ Неудач: <b>{failed}</b>"
        )
        await send_log("sherlock", log_text)
    elif success == 0 and failed > 0:
         log_text = f"❌ <b>Snoser Fail</b>\nЦель: {target_bot}, Юзер: {user_id}. Все сессии отклонили запрос."
         await send_log("sherlock", log_text)

    return success, failed

# ====================== NARKO(PAV) BOT DELETER ======================
async def run_narko_deleter(target_bot: str, user_id: int, username: str):
    if await is_own_bot(target_bot):
        logger.warning(f"🚫 Попытка репорта на своего бота: {target_bot} от пользователя {user_id}")
        return 0, 0

    if await is_bot_already_reported(target_bot):
        return -1, 0

    session_files = [f for f in os.listdir('sessions') if f.endswith('.session') and "AU_report" not in f]
    if not session_files:
        return 0, -2

    target_formatted = target_bot if target_bot.startswith('@') else f'@{target_bot}'
    
    entity_found = False
    checked_sessions = set()
    
    for attempt in range(7):
        available_sessions = [s for s in session_files if s not in checked_sessions]
        if not available_sessions: break
            
        test_sess_file = random.choice(available_sessions)
        checked_sessions.add(test_sess_file)
        test_sess_name = test_sess_file.replace('.session', '')
        test_client = TelegramClient(test_sess_name, API_ID, API_HASH)
        
        try:
            await test_client.connect()
            await test_client.get_input_entity(target_formatted)
            entity_found = True
            break
        except Exception:
            pass
        finally:
            await test_client.disconnect()

    if not entity_found:
        return 0, -2

    random.shuffle(session_files)
    selected_sessions = session_files[:MAX_SESSIONS]

    success, failed = 0, 0
    report_text = "Этот бот используется для продажи наркотических веществ, таких как: мефедрон, гашишь. Прошу принять меры"

    for sess_file in selected_sessions:
        sess_name = sess_file.replace('.session', '')
        client = TelegramClient(sess_name, API_ID, API_HASH)
        try:
            await client.connect()
            peer = await client.get_input_entity(target_formatted)
            await client(ReportPeerRequest(
                peer=peer,
                reason=InputReportReasonIllegalDrugs(),
                message=report_text
            ))
            success += 1
            await asyncio.sleep(8)
        except Exception:
            failed += 1
        finally:
            await client.disconnect()

    if success > 0:
        await add_report_stat(user_id)
        await add_to_reported_history(target_bot)
        
        log_text = (
            f"⛔ <b>Narko(pav) Bot Deleter</b>\n\n"
            f"👤 Пользователь: @{username} (ID: <code>{user_id}</code>)\n"
            f"🎯 Цель: <b>{target_bot}</b>\n"
            f"✅ Успешно: <b>{success}</b>/{len(selected_sessions)}\n"
            f"❌ Неудач: <b>{failed}</b>"
        )
        await send_log("sherlock", log_text)
    elif success == 0 and failed > 0:
         log_text = f"❌ <b>Narko Fail</b>\nЦель: {target_bot}, Юзер: {user_id}. Все сессии отклонили запрос."
         await send_log("sherlock", log_text)

    return success, failed
# Логика AU запуск
@router.callback_query(F.data == "au_start")
async def au_start(call: CallbackQuery, state: FSMContext):
    if call.message.photo:
        await call.message.edit_caption(caption="Введите ссылку (t.me/username или @username):")
    else:
        await call.message.edit_text("Введите ссылку (t.me/username или @username):")
    await state.set_state(UserStates.waiting_for_au_target)

@router.message(UserStates.waiting_for_au_target)
async def au_target(message: Message, state: FSMContext):
    target = message.text.strip()
    
    if target.startswith("@"):
        target = f"t.me/{target[1:]}"
    elif target.startswith("https://t.me/"):
        target = target.replace("https://", "")
    elif target.startswith("http://t.me/"):
        target = target.replace("http://", "")

    await state.update_data(target=target)
    await message.answer(f"✅ Принято: <b>{target}</b>\n\n📝 Введите текст жалобы:")
    await state.set_state(UserStates.waiting_for_au_reason)

@router.message(UserStates.waiting_for_au_reason)
async def au_reason(message: Message, state: FSMContext):
    await state.update_data(reason=message.text)
    data = await state.get_data()
    
    confirm_text = (
        f"🎯 <b>Цель:</b> {data['target']}\n"
        f"📝 <b>Жалоба:</b> {data['reason']}\n\n"
        f"❓ <b>Отправить эту жалобу в очередь?</b>"
    )
    
    await message.answer(confirm_text, reply_markup=get_confirm_menu())
    await state.set_state(UserStates.waiting_for_au_confirm)

@router.callback_query(UserStates.waiting_for_au_confirm, F.data == "au_confirm_yes")
async def au_confirm_yes(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    # Передаем также токен бота, чтобы воркер знал, с какого бота отправлять ответ
    async with report_queue_lock:
        await au_report_queue.put((call.from_user.id, data['target'], data['reason'], call.bot.token))
    await call.message.edit_text("🚀 Успешно! Добавлено в очередь AU Report.")
    await state.clear()

@router.callback_query(UserStates.waiting_for_au_confirm, F.data == "au_confirm_no")
async def au_confirm_no(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("❌ Отправка жалобы отменена.")
    await state.clear()

# === TIDA REPORT ===
@router.callback_query(F.data == "tida_start")
async def tida_start(call: CallbackQuery, state: FSMContext):
    if call.message.photo:
        await call.message.edit_caption(caption="Введите ссылку (t.me/username или @username):")
    else:
        await call.message.edit_text("Введите ссылку (t.me/username или @username):")
    await state.set_state(UserStates.waiting_for_tida_target)

@router.message(UserStates.waiting_for_tida_target)
async def tida_target(message: Message, state: FSMContext):
    target = message.text.strip()
    
    if target.startswith("@"):
        target = f"t.me/{target[1:]}"
    elif target.startswith("https://t.me/"):
        target = target.replace("https://", "")
    elif target.startswith("http://t.me/"):
        target = target.replace("http://", "")

    await state.update_data(target=target)
    await message.answer(f"✅ Принято: <b>{target}</b>\n\n📝 Введите текст жалобы:")
    await state.set_state(UserStates.waiting_for_tida_reason)

@router.message(UserStates.waiting_for_tida_reason)
async def tida_reason(message: Message, state: FSMContext):
    await state.update_data(reason=message.text)
    data = await state.get_data()
    
    confirm_text = (
        f"🎯 <b>Цель:</b> {data['target']}\n"
        f"📝 <b>Жалоба:</b> {data['reason']}\n\n"
        f"❓ <b>Отправить эту жалобу в очередь?</b>"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да", callback_data="tida_confirm_yes"),
         InlineKeyboardButton(text="❌ Нет", callback_data="tida_confirm_no")]
    ])
    
    await message.answer(confirm_text, reply_markup=kb)
    await state.set_state(UserStates.waiting_for_tida_confirm)

@router.callback_query(UserStates.waiting_for_tida_confirm, F.data == "tida_confirm_yes")
async def tida_confirm_yes(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    # Передаем также токен бота, чтобы воркер знал, с какого бота отправлять ответ
    async with report_queue_lock:
        await tida_report_queue.put((call.from_user.id, data['target'], data['reason'], call.bot.token))
    await call.message.edit_text("🚀 Успешно! Добавлено в очередь TIDA Report.")
    await state.clear()

@router.callback_query(UserStates.waiting_for_tida_confirm, F.data == "tida_confirm_no")
async def tida_confirm_no(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("❌ Отправка жалобы отменена.")
    await state.clear()

# --- ПРОФИЛЬ И АДМИНКА ---
@router.callback_query(F.data == "profile")
async def profile(call: CallbackQuery, state: FSMContext):
    await state.clear()
    users = await get_users()
    u = users.get(call.from_user.id, {"sub_until": "0", "reports": 0, "tokens": []})
    sub = u['sub_until']
    is_sub = await has_sub(call.from_user.id)
    status = "Активна" if is_sub else "Нет"
    tokens = u.get("tokens", [])
    
    text = (f"👤 <b>Профиль</b>\n"
            f"ID: <code>{call.from_user.id}</code>\n"
            f"Подписка: {status} ({sub})\n"
            f"Репортов: {u['reports']}\n"
            f"Зеркал: {len(tokens)}/{MAX_MIRRORS}")
            
    kb = [
        [InlineKeyboardButton(text="➕ Создать зеркало", callback_data="add_mirror")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")]
    ]
    markup = InlineKeyboardMarkup(inline_keyboard=kb)
    
    if call.message.photo:
        await call.message.edit_caption(caption=text, reply_markup=markup)
    else:
        await call.message.edit_text(text, reply_markup=markup)

@router.callback_query(F.data == "admin_panel")
async def admin_panel(call: CallbackQuery):
    if call.from_user.id in ALLOWED_USERS:
        if call.message.photo:
            await call.message.edit_caption(caption="<b>🛠 Админка</b>", reply_markup=get_admin_menu())
        else:
            await call.message.edit_text("<b>🛠 Админка</b>", reply_markup=get_admin_menu())

@router.callback_query(F.data == "adm_broadcast")
async def adm_broadcast(call: CallbackQuery, state: FSMContext):
    if call.message.photo:
        await call.message.edit_caption(caption="Введите текст рассылки для ВСЕХ:")
    else:
        await call.message.edit_text("Введите текст рассылки для ВСЕХ:")
    await state.set_state(AdminStates.waiting_for_broadcast)

@router.message(AdminStates.waiting_for_broadcast)
async def process_broadcast(message: Message, state: FSMContext):
    users = await get_users()
    await message.answer(f"🚀 Начинаю рассылку на {len(users)} чел...")
    for uid in users.keys():
        try:
            await message.bot.send_message(uid, message.text)
            await asyncio.sleep(0.05)
        except: pass
    await message.answer("✅ Готово!")
    await state.clear()

@router.callback_query(F.data == "adm_sub")
async def adm_sub(call: CallbackQuery, state: FSMContext):
    if call.message.photo:
        await call.message.edit_caption(caption="Введите ID:")
    else:
        await call.message.edit_text("Введите ID:")
    await state.set_state(AdminStates.waiting_for_sub_id)

@router.message(AdminStates.waiting_for_sub_id)
async def adm_sub_id(message: Message, state: FSMContext):
    await state.update_data(sid=message.text)
    await message.answer("Дни (0 - навсегда):")
    await state.set_state(AdminStates.waiting_for_sub_time)

@router.callback_query(F.data == "buy_sub")
async def buy_subscription(call: CallbackQuery):
    text = (
        "💎 <b>Покупка подписки</b>\n\n"
        "Для приобретения доступа к боту, обратитесь к админам:\n\n"
        "1.@Peredaliky\n"
        "2.@MilitaryMonesy\n\n"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="@Peredaliky", url="https://t.me/Peredaliky")],
        [InlineKeyboardButton(text="@MilitaryMonesy", url="https://t.me/MilitaryMonesy")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")]
    ])

    if call.message.photo:
        await call.message.edit_caption(caption=text, reply_markup=kb)
    else:
        await call.message.edit_text(text, reply_markup=kb)

@router.message(AdminStates.waiting_for_sub_time)
async def adm_sub_time(message: Message, state: FSMContext):
    data = await state.get_data()
    users = await get_users()
    admin_id = message.from_user.id
    try:
        sid = int(data['sid'])
    except ValueError:
        return await message.answer("❌ Ошибка: ID должен быть числом.")
        
    if sid not in users: 
        users[sid] = {"name": "User", "reports": 0, "tokens": [], "requests": 0, "blocked": 0}
    
    old_sub = users[sid].get("sub_until", "0")
    
    if message.text == "0": 
        users[sid]["sub_until"] = "∞"
    else:
        try:
            d = datetime.datetime.now() + datetime.timedelta(days=int(message.text))
            users[sid]["sub_until"] = d.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            return await message.answer("❌ Ошибка: Дни должны быть числом.")
            
    await save_users(users)
    await message.answer("✅ Выдано!")
    
    # Логирование выдачи подписки
    log_msg = f"🟢 <b>Подписка выдана</b>\n👤 Админ: {admin_id}\n👥 Пользователь: {sid}\n📅 До: {users[sid]['sub_until']}"
    await send_log("mail", log_msg)
    
    await state.clear()

@router.callback_query(F.data == "adm_unsub")
async def adm_unsub(call: CallbackQuery, state: FSMContext):
    if call.message.photo:
        await call.message.edit_caption(caption="Введите ID пользователя для снятия подписки:")
    else:
        await call.message.edit_text("Введите ID пользователя для снятия подписки:")
    await state.set_state(AdminStates.waiting_for_unsub_id)

@router.message(AdminStates.waiting_for_unsub_id)
async def process_unsub(message: Message, state: FSMContext):
    try:
        sid = int(message.text)
        users = await get_users()
        admin_id = message.from_user.id
        if sid in users:
            old_sub = users[sid]["sub_until"]
            users[sid]["sub_until"] = "0"
            await save_users(users)
            await message.answer(f"✅ Подписка у пользователя <code>{sid}</code> успешно забрана!")
            # Логирование
            log_msg = f"🟡 <b>Подписка забрана</b>\n👤 Админ: {admin_id}\n👥 Пользователь: {sid}\n📅 Было: {old_sub}"
            await send_log("mail", log_msg)
        else:
            await message.answer("❌ Пользователь с таким ID не найден в базе данных.")
    except ValueError:
        await message.answer("❌ Ошибка: ID должен быть числом.")
    await state.clear()

@router.callback_query(F.data == "adm_ban")
async def adm_ban(call: CallbackQuery, state: FSMContext):
    if call.message.photo:
        await call.message.edit_caption(caption="Введите ID пользователя для блокировки:")
    else:
        await call.message.edit_text("Введите ID пользователя для блокировки:")
    await state.set_state(AdminStates.waiting_for_ban_id)

@router.message(AdminStates.waiting_for_ban_id)
async def process_ban(message: Message, state: FSMContext):
    try:
        sid = int(message.text)
        users = await get_users()
        admin_id = message.from_user.id
        
        if sid in users:
            # Снимаем подписку
            users[sid]["sub_until"] = "0"
            # Блокируем пользователя
            users[sid]["blocked"] = 1
            await save_users(users)
            
            # Останавливаем все зеркала пользователя
            tokens_to_stop = users[sid].get("tokens", [])[:]
            for token in tokens_to_stop:
                if token:
                    await stop_mirror_bot(token)
            
            await message.answer(f"🚫 Пользователь <code>{sid}</code> заблокирован!\nВсе зеркала остановлены, подписка снята.")
            
            # Логирование блокировки
            log_msg = f"🔴 <b>Пользователь заблокирован</b>\n👤 Админ: {admin_id}\n👥 Заблокирован: {sid}"
            await send_log("ban", log_msg)
        else:
            await message.answer("❌ Пользователь с таким ID не найден в базе данных.")
    except ValueError:
        await message.answer("❌ Ошибка: ID должен быть числом.")
    await state.clear()

@router.callback_query(F.data == "adm_stats")
async def adm_stats(call: CallbackQuery):
    users = await get_users()
    total_users = len(users)
    active_subs = 0
    for uid in users:
        if await has_sub(uid):
            active_subs += 1
    total_reports = sum(user.get("reports", 0) for user in users.values())
    total_mirrors = sum(len(user.get("tokens", [])) for user in users.values())
    
    text = (
        "📊 <b>Статистика бота</b>\n\n"
        f"👥 Всего пользователей: <b>{total_users}</b>\n"
        f"💎 Активных подписок: <b>{active_subs}</b>\n"
        f"📢 Всего отправлено репортов: <b>{total_reports}</b>\n"
        f"🪞 Запущено зеркал: <b>{total_mirrors}</b>"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel")]
    ])
    
    if call.message.photo:
        await call.message.edit_caption(caption=text, reply_markup=kb)
    else:
        await call.message.edit_text(text, reply_markup=kb)

# --- ПРОВЕРКА СЕССИЙ ЧЕРЕЗ @spambot ---
@router.callback_query(F.data == "check_sessions")
async def check_sessions_callback(call: CallbackQuery):
    if call.from_user.id not in ALLOWED_USERS:
        return
    
    await call.answer("🔍 Начинаю проверку сессий через @spambot...")
    
    session_files = [f for f in os.listdir('sessions') if f.endswith('.session') and "AU_report" not in f]
    if not session_files:
        return await call.answer("❌ Сессии не найдены!", show_alert=True)
    
    valid_sessions = []
    invalid_sessions = []
    
    async def check_single_session(sess_file):
        """Проверяет одну сессию и возвращает результат"""
        sess_name = sess_file.replace('.session', '')
        
        try:
            client = TelegramClient(f"sessions/{sess_name}", API_ID, API_HASH)
            await client.connect()
            # Отправляем сообщение @spambot
            try:
                await client.send_message("spambot", "/start")
                await asyncio.sleep(2)
                # Получаем последние сообщения
                messages = await client.get_messages("spambot", limit=5)
                
                is_valid = False
                for msg in messages:
                    if msg.text and ("limit" not in msg.text.lower() and "blocked" not in msg.text.lower()):
                        is_valid = True
                        break
                
                await client.disconnect()
                
                if is_valid:
                    logger.info(f"✅ Сессия валидна: {sess_file}")
                    return (sess_file, True)
                else:
                    logger.warning(f"❌ Сессия невалидна: {sess_file}")
                    return (sess_file, False)
            except Exception as e:
                await client.disconnect()
                logger.error(f"❌ Ошибка проверки сессии {sess_file}: {e}")
                return (sess_file, False)
        except Exception as e:
            logger.error(f"❌ Ошибка подключения/инициализации сессии {sess_file}: {e}")
            return (sess_file, False)
    
    # Запускаем все проверки параллельно в отдельных потоках
    tasks = [check_single_session(sess_file) for sess_file in session_files]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for result in results:
        if isinstance(result, Exception):
            continue
        sess_file, is_valid = result
        if is_valid:
            valid_sessions.append(sess_file)
        else:
            invalid_sessions.append(sess_file)
    
    # Перемещаем невалидные сессии в другую папку
    if invalid_sessions:
        os.makedirs("invalid_sessions", exist_ok=True)
        for sess_file in invalid_sessions:
            try:
                # Перемещаем файл сессии и .session из папки sessions
                if os.path.exists(f"sessions/{sess_file}"):
                    os.rename(f"sessions/{sess_file}", f"invalid_sessions/{sess_file}")
                if os.path.exists(f"sessions/{sess_file}.session"):
                    os.rename(f"sessions/{sess_file}.session", f"invalid_sessions/{sess_file}.session")
            except Exception as e:
                logger.error(f"Ошибка перемещения сессии {sess_file}: {e}")
    
    result_text = (
        f"🔍 <b>Результаты проверки сессий</b>\n\n"
        f"✅ Валидных: {len(valid_sessions)}\n"
        f"❌ Невалидных: {len(invalid_sessions)}\n"
        f"📁 Невалидные перемещены в: invalid_sessions/"
    )
    
    await send_log("other", result_text)
    await call.answer(f"✅ Проверено: {len(session_files)}\nВалидных: {len(valid_sessions)}\nНевалидных: {len(invalid_sessions)}", show_alert=True)

# --- ПРОВЕРКА ПОЧТ ---
@router.callback_query(F.data == "check_emails")
async def check_emails_callback(call: CallbackQuery):
    if call.from_user.id not in ALLOWED_USERS:
        return
    
    await call.answer("📧 Начинаю проверку почт...")
    
    if not os.path.exists(EMAILS_FILE) or os.stat(EMAILS_FILE).st_size == 0:
        return await call.answer("❌ Файл emails.txt пуст или не существует!", show_alert=True)
    
    valid_emails = []
    invalid_emails = []
    
    with open(EMAILS_FILE, "r", encoding="utf-8") as f:
        emails = [line.strip() for line in f if line.strip()]
    
    for email_line in emails:
        parts = email_line.split(":")
        if len(parts) < 3:
            invalid_emails.append(email_line)
            continue
        
        email = parts[0]
        password = parts[1]
        smtp_server = parts[2] if len(parts) > 2 else "smtp.gmail.com"
        
        try:
            # Отправляем письмо сами себе
            msg = MIMEMultipart()
            msg['From'] = email
            msg['To'] = email
            msg['Subject'] = "Self-Test Email"
            body = "This is a self-test email to verify account validity."
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(smtp_server, 587)
            server.starttls()
            server.login(email, password)
            server.send_message(msg)
            server.quit()
            
            valid_emails.append(email_line)
            logger.info(f"✅ Почта валидна: {email}")
        except Exception as e:
            invalid_emails.append(email_line)
            logger.warning(f"❌ Почта невалидна: {email} - {e}")
    
    # Сохраняем невалидные почты в отдельный файл
    if invalid_emails:
        with open("invalid_emails.txt", "w", encoding="utf-8") as f:
            for email_line in invalid_emails:
                f.write(f"{email_line}\n")
    
    result_text = (
        f"📧 <b>Результаты проверки почт</b>\n\n"
        f"✅ Валидных: {len(valid_emails)}\n"
        f"❌ Невалидных: {len(invalid_emails)}\n"
        f"📄 Невалидные сохранены в: invalid_emails.txt"
    )
    
    await send_log("mail", result_text)
    await call.answer(f"✅ Проверено: {len(emails)}\nВалидных: {len(valid_emails)}\nНевалидных: {len(invalid_emails)}", show_alert=True)

# --- ДОБАВИТЬ СЕССИИ ---
@router.callback_query(F.data == "add_sessions")
async def add_sessions_callback(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ALLOWED_USERS:
        return
    
    if call.message.photo:
        await call.message.edit_caption(caption="📥 <b>Добавить сессии</b>\n\nОтправьте файлы сессий (.session) в чат.\nПосле отправки всех файлов нажмите /cancel для завершения.")
    else:
        await call.message.edit_text("📥 <b>Добавить сессии</b>\n\nОтправьте файлы сессий (.session) в чат.\nПосле отправки всех файлов нажмите /cancel для завершения.")
    
    await state.set_state(AdminStates.waiting_for_session_file)

@router.message(AdminStates.waiting_for_session_file, F.document)
async def process_session_files(message: Message, state: FSMContext):
    document = message.document
    
    if not document.file_name.endswith('.session'):
        return await message.answer("❌ Пожалуйста, отправляйте только файлы .session")
    
    # Скачиваем файл
    file_path = f"sessions/{document.file_name}"
    os.makedirs("sessions", exist_ok=True)
    
    try:
        await message.bot.download(document, destination=file_path)
        
        # Проверяем сессию в отдельном потоке
        async def check_and_validate():
            sess_name = document.file_name.replace('.session', '')
            client = TelegramClient(f"sessions/{sess_name}", API_ID, API_HASH)
            
            try:
                await client.connect()
                # Проверяем валидность через @spambot
                try:
                    await client.send_message("spambot", "/start")
                    await asyncio.sleep(2)
                    messages = await client.get_messages("spambot", limit=5)
                    
                    is_valid = False
                    for msg in messages:
                        if msg.text and ("limit" not in msg.text.lower() and "blocked" not in msg.text.lower()):
                            is_valid = True
                            break
                    
                    await client.disconnect()
                    
                    if is_valid:
                        logger.info(f"✅ Сессия добавлена: {document.file_name}")
                        return True, f"✅ Сессия <b>{document.file_name}</b> успешно добавлена!"
                    else:
                        os.remove(file_path)
                        logger.warning(f"❌ Сессия невалидна: {document.file_name}")
                        return False, f"❌ Сессия <b>{document.file_name}</b> невалидна и удалена."
                except Exception as e:
                    os.remove(file_path)
                    logger.error(f"❌ Ошибка проверки сессии {document.file_name}: {e}")
                    return False, f"❌ Ошибка проверки сессии: {e}"
            except Exception as e:
                os.remove(file_path)
                logger.error(f"❌ Ошибка подключения сессии {document.file_name}: {e}")
                return False, f"❌ Ошибка подключения сессии: {e}"
        
        # Запускаем проверку в отдельном потоке
        is_valid, response_text = await check_and_validate()
        
        if is_valid:
            await send_log("other", f"📥 <b>Сессия добавлена</b>\n📄 Файл: {document.file_name}\n👤 Админ: {message.from_user.id}")
        
        await message.answer(response_text)
            
    except Exception as e:
        logger.error(f"Ошибка скачивания файла: {e}")
        await message.answer(f"❌ Ошибка скачивания файла: {e}")

@router.message(AdminStates.waiting_for_session_file, F.text)
async def cancel_session_upload(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("✅ Загрузка сессий завершена.")

# --- КОНФИГУРАЦИЯ БОЛЬШЕ НЕ ТРЕБУЕТ ПРОКСИ ---

# --- ИСПРАВЛЕННАЯ ФУНКЦИЯ ПРОВЕРКИ СЕТИ ---
async def check_proxy() -> bool:
    """Проверяет доступность серверов Telegram напрямую перед запуском."""
    logger.info("Проверяю прямое подключение к Telegram...")
    
    timeout = aiohttp.ClientTimeout(total=5)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get("https://api.telegram.org", ssl=False) as response:
                if response.status == 200:
                    logger.info("Прямое подключение успешно установлено!")
                    return True
                else:
                    logger.error(f"Сервер ответил кодом {response.status}, Telegram недоступен.")
                    return False
    except Exception as e:
        logger.critical(f"Ошибка подключения: {e}. Нет связи с Telegram.")
        return False
from aiogram.client.session.aiohttp import AiohttpSession  # Если используется в коде
# import socks  # Больше не нужен, можно удалить
# --- ИСПРАВЛЕННЫЙ МЕТОД MAIN ---
import asyncio
# ... (остальные ваши импорты остаются без изменений)

async def run_single_bot_polling(bot: Bot, dp: Dispatcher):
    """
    Запускает polling для одного конкретного бота.
    Это позволяет изолировать ошибки и гарантировать параллельный запуск.
    """
    try:
        logger.info(f"🟢 Запуск polling для бота: @{bot.username} (ID: {bot.id})")
        # start_polling блокирует выполнение внутри задачи, но так как задач много, они работают параллельно
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"🔴 Ошибка polling для бота @{bot.username}: {e}")
    finally:
        logger.info(f"⚪ Polling остановлен для бота @{bot.username}")

async def main():
    await init_db()
    
    # Инициализируем клиент логов перед запуском ботов
    await log_client.start()
    
    asyncio.create_task(au_report_worker())
    mirror_bots = await load_all_mirrors()
    all_bots = [main_bot] + mirror_bots
    
    logger.info("Бот и зеркала запущены. Логирование активно.")
    await main_dp.start_polling(*all_bots)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Боты остановлены.")

