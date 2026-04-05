import asyncio
import json
import os
import random
import string
import time
import aiohttp
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardRemove, BufferedInputFile,
)
from aiogram.client.default import DefaultBotProperties

# ==================== КОНФИГУРАЦИЯ ====================
BOT_TOKEN = "8788855800:AAH1Nwshz9JXi1t1AEmGE6mnY5E17bJWxfo"
ADMIN_IDS = [8122843073]
API_KEY = "5e53e868BB30B83374d4098d0018978e"
API_URL = "https://hero-sms.com/stubs/handler_api.php"
DB_PATH = "db.json"
SUPPORT_LINK = "https://t.me/support"

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())


# ==================== БАЗА ДАННЫХ ====================
def load_db() -> dict:
    if not os.path.exists(DB_PATH):
        return {
            "users": {},
            "settings": {},
            "transactions": [],
            "activations": [],
            "topup_requests": {},
            "withdraw_requests": {},
            "cached_services": {},
            "promo_codes": {},
            "notes": {},
            "warnings": {},
            "activity_log": [],
            "featured_services": {
                "tg": {"name": "Telegram", "price": 50, "description": "Telegram"},
                "claude": {"name": "Claude", "price": 100, "description": "Claude"},
                "tikitok": {"name": "TikTok", "price": 30, "description": "TikTok"},
            },
            "custom_ref_codes": {},
            "support_tickets": {},
            "orders": {},
            "required_channels": {},
            "subscription_text": "📰 Для использования бота подпишитесь на наши каналы:",
            "custom_service_prices": {},
        }
    with open(DB_PATH, "r", encoding="utf-8") as f:
        db = json.load(f)
    
    # Инициализируем новые поля если их нет
    if "featured_services" not in db:
        db["featured_services"] = {
            "tg": {"name": "Telegram", "price": 50, "description": "Telegram"},
            "claude": {"name": "Claude", "price": 100, "description": "Claude"},
            "tikitok": {"name": "TikTok", "price": 30, "description": "TikTok"},
        }
    if "custom_ref_codes" not in db:
        db["custom_ref_codes"] = {}
    if "support_tickets" not in db:
        db["support_tickets"] = {}
    if "orders" not in db:
        db["orders"] = {}
    if "required_channels" not in db:
        db["required_channels"] = {}
    if "subscription_text" not in db:
        db["subscription_text"] = "📰 Для использования бота подпишитесь на наши каналы:"
    if "custom_service_prices" not in db:
        db["custom_service_prices"] = {}
    
    return db


def save_db(db: dict):
    with open(DB_PATH, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)


def get_user(uid: int) -> dict:
    db = load_db()
    uid_str = str(uid)
    if uid_str not in db["users"]:
        db["users"][uid_str] = {
            "id": uid,
            "balance": 0.0,
            "ref_balance": 0.0,
            "ref_code": "".join(random.choices(string.ascii_lowercase + string.digits, k=8)),
            "referrer": None,
            "referrals": [],
            "total_spent": 0.0,
            "total_topup": 0.0,
            "activations_count": 0,
            "success_activations": 0,
            "joined_at": datetime.now().isoformat(),
            "username": None,
            "first_name": None,
            "banned": False,
            "frozen": False,
            "active_numbers": [],
            "warnings_count": 0,
            "vip": False,
            "last_activity": datetime.now().isoformat(),
            "notes": "",
            "promo_used": [],
        }
        save_db(db)
    return db["users"][uid_str]


def save_user(uid: int, data: dict):
    db = load_db()
    db["users"][str(uid)] = data
    save_db(db)


def get_settings() -> dict:
    db = load_db()
    defaults = {
        "ref_percent": 10.0,
        "min_withdraw": 500,
        "min_topup": 10,
        "bank_name": "Сбербанк",
        "bank_requisites": "4276 1234 5678 9012",
        "bank_owner": "Иван И.",
        "price_markup": 0.0,
        "welcome_text": None,
        "support_link": SUPPORT_LINK,
        "cryptobot_token": "",
        "maintenance": False,
        "bot_name": "Stuffiny SMS",
        "default_country": "0",
        "max_active_numbers": 10,
        "auto_cancel_minutes": 20,
        "vip_discount": 5.0,
    }
    s = db.get("settings", {})
    for k, v in defaults.items():
        if k not in s:
            s[k] = v
    db["settings"] = s
    save_db(db)
    return s


def save_settings(s: dict):
    db = load_db()
    db["settings"] = s
    save_db(db)


def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


def init_test_channels():
    db = load_db()
    if "required_channels" not in db:
        db["required_channels"] = {}
    
    # Добавляем тестовый канал только если его еще нет
    if "stuffinydev" not in db["required_channels"]:
        db["required_channels"]["stuffinydev"] = {
            "username": "stuffinydev",
            "name": "Stuffiny Dev",
            "url": "https://t.me/stuffinydev"
        }
        save_db(db)
        print("✅ Добавлен тестовый канал: @stuffinydev")


def get_subscription_channels() -> list:
    """Получить список обязательных каналов"""
    db = load_db()
    channels = db.get("required_channels", {})
    return [
        {
            "username": ch["username"],
            "name": ch["name"],
            "url": ch["url"]
        }
        for ch in channels.values()
    ]


async def check_subscription(uid: int) -> bool:
    """Проверить подписку пользователя на все обязательные каналы"""
    channels = get_subscription_channels()
    
    # Если каналов нет, считаем что подписка не требуется
    if not channels:
        return True
    
    # Проверяем подписку на каждый канал
    for ch in channels:
        try:
            member = await bot.get_chat_member(f"@{ch['username']}", uid)
            # Проверяем статус: member, administrator, creator
            if member.status not in ["member", "administrator", "creator"]:
                return False
        except Exception:
            # Если не удалось проверить, считаем что не подписан
            return False
    
    return True


def e(emoji_id: str, fallback: str = "•") -> str:
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'

async def show_subscription_required(message: Message | CallbackQuery) -> bool:
    """Показывает требование к подписке. Возвращает True если пользователь не подписан"""
    uid = message.from_user.id
    if await check_subscription(uid):
        return False
    
    db = load_db()
    channels = get_subscription_channels()
    sub_text = db.get("subscription_text", "Для использования бота подпишитесь на наши каналы:")
    
    # Цитируемый текст с премиум эмодзи
    quoted_text = f'<blockquote>{e("6039422865189638057", "📰")} {sub_text}</blockquote>'
    
    kb_rows = []
    for ch in channels:
        kb_rows.append([InlineKeyboardButton(
            text="Подписаться",
            url=ch["url"],
            icon_custom_emoji_id="5260268501515377807"
        )])
    kb_rows.append([InlineKeyboardButton(
        text="Проверить подписку",
        callback_data="check_subscription",
        icon_custom_emoji_id="5260416304224936047"
    )])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    
    if isinstance(message, Message):
        await message.answer(quoted_text, reply_markup=kb)
    else:
        await message.message.answer(quoted_text, reply_markup=kb)
    
    return True

def add_transaction(uid: int, amount: float, tx_type: str, note: str = ""):
    db = load_db()
    if "transactions" not in db:
        db["transactions"] = []
    db["transactions"].append({
        "uid": uid,
        "amount": amount,
        "type": tx_type,
        "note": note,
        "date": datetime.now().isoformat(),
    })
    save_db(db)


def log_activity(uid: int, action: str):
    db = load_db()
    if "activity_log" not in db:
        db["activity_log"] = []
    db["activity_log"].append({
        "uid": uid,
        "action": action,
        "date": datetime.now().isoformat(),
    })
    if len(db["activity_log"]) > 10000:
        db["activity_log"] = db["activity_log"][-5000:]
    save_db(db)


def get_stats() -> dict:
    db = load_db()
    now = datetime.now()
    txs = db.get("transactions", [])
    users = db.get("users", {})

    def income_period(days):
        cutoff = now - timedelta(days=days)
        return sum(
            t["amount"] for t in txs
            if t["type"] == "topup" and datetime.fromisoformat(t["date"]) >= cutoff
        )

    new_today = sum(
        1 for u in users.values()
        if datetime.fromisoformat(u.get("joined_at", "2000-01-01")) >= now - timedelta(days=1)
    )
    new_week = sum(
        1 for u in users.values()
        if datetime.fromisoformat(u.get("joined_at", "2000-01-01")) >= now - timedelta(days=7)
    )

    return {
        "users_total": len(users),
        "users_banned": sum(1 for u in users.values() if u.get("banned")),
        "users_frozen": sum(1 for u in users.values() if u.get("frozen")),
        "users_vip": sum(1 for u in users.values() if u.get("vip")),
        "new_today": new_today,
        "new_week": new_week,
        "income_day": income_period(1),
        "income_week": income_period(7),
        "income_month": income_period(30),
        "income_year": income_period(365),
        "income_all": sum(t["amount"] for t in txs if t["type"] == "topup"),
        "admin_adds": sum(t["amount"] for t in txs if t["type"] == "admin_add"),
        "total_withdrawn": sum(t["amount"] for t in txs if t["type"] == "withdraw"),
        "pending_topups": len([v for v in db.get("topup_requests", {}).values() if v["status"] == "pending"]),
        "pending_withdraws": len([v for v in db.get("withdraw_requests", {}).values() if v["status"] == "pending"]),
    }


def get_user_display(u: dict) -> str:
    if u.get("username"):
        return f"@{u['username']}"
    elif u.get("first_name"):
        return u["first_name"]
    else:
        return str(u["id"])


# ==================== API SMSFASTAPI ====================
async def api_get(params: dict) -> str:
    params["api_key"] = API_KEY
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(API_URL, params=params, timeout=aiohttp.ClientTimeout(total=30)) as r:
                text = await r.text()
                return text.strip()
    except asyncio.TimeoutError:
        return "ERROR_TIMEOUT"
    except Exception as ex:
        return f"ERROR:{ex}"


async def api_get_services(country: str = None) -> list:
    """Получить список сервисов с ценами для страны"""
    try:
        # Получаем список всех сервисов
        result = await api_get({"action": "getServicesList"})
        
        if not result.startswith("{"):
            return []
        data = json.loads(result)
        
        # Проверяем статус ответа
        if data.get("status") != "success":
            return []
        
        services_list = data.get("services", [])
        
        if not services_list or not country:
            return services_list
        
        # Получаем цены для выбранной страны
        prices_result = await api_get({"action": "getPrices", "country": country})
        
        if not prices_result.startswith("[") and not prices_result.startswith("{"):
            return services_list
        
        prices_data = json.loads(prices_result)
        
        # API может вернуть массив или объект
        if isinstance(prices_data, list) and len(prices_data) > 0:
            prices_data = prices_data[0] if prices_data else {}
        
        # Структура: {"country_id": {"service_code": {"cost": ..., "count": ...}}}
        # или просто {"service_code": {"cost": ..., "count": ...}}
        country_prices = prices_data.get(str(country), prices_data)
        if not isinstance(country_prices, dict):
            country_prices = prices_data
        
        enriched = []
        for service in services_list:
            code = service.get("code", "")
            price_info = country_prices.get(code, {})
            
            # Проверяем что price_info это словарь
            if isinstance(price_info, dict):
                enriched.append({
                    "code": code,
                    "name": service.get("name", code),
                    "cost": float(price_info.get("cost", 0)),
                    "count": int(price_info.get("count", 0)),
                })
            else:
                enriched.append({
                    "code": code,
                    "name": service.get("name", code),
                    "cost": 0,
                    "count": 0,
                })
        return enriched
    except Exception as ex:
        print(f"Error in api_get_services: {ex}")
        import traceback
        traceback.print_exc()
        return []
        return []


async def fetch_services_hardcoded() -> dict:
    """Возвращает встроенный список сервисов в качестве резервного"""
    try:
        hardcoded = {
            "claude": {"name": "Claude", "price": 100, "count": 10},
            "tg": {"name": "Telegram", "price": 50, "count": 10},
            "wa": {"name": "WhatsApp", "price": 60, "count": 10},
            "ig": {"name": "Instagram", "price": 70, "count": 8},
            "fb": {"name": "Facebook", "price": 55, "count": 10},
            "vk": {"name": "VKontakte", "price": 40, "count": 15},
            "ok": {"name": "Odnoklassniki", "price": 35, "count": 12},
            "tiktok": {"name": "TikTok", "price": 30, "count": 20},
            "discord": {"name": "Discord", "price": 65, "count": 10},
            "youtube": {"name": "YouTube", "price": 45, "count": 15},
        }
        return hardcoded
    except Exception as ex:
        print(f"Error in fetch_services_hardcoded: {ex}")
        return {}


def get_service_priority(code: str) -> int:
    """Возвращает приоритет сервиса (меньше = выше в списке, по популярности)"""
    priority_map = {
        "claude": 1,      # 1️⃣ Claude - самый популярный
        "tg": 2,          # 2️⃣ Telegram
        "wa": 3,          # 3️⃣ WhatsApp
        "ig": 4,          # 4️⃣ Instagram
        "fb": 5,          # 5️⃣ Facebook
        "vk": 6,          # 6️⃣ VKontakte
        "tiktok": 7,      # 7️⃣ TikTok
        "discord": 8,     # 8️⃣ Discord
        "youtube": 9,     # 9️⃣ YouTube
    }
    return priority_map.get(code.lower(), 999)


def capitalize_service_name(name: str) -> str:
    """Капитализирует название сервиса (первая буква заглавная)"""
    if not name:
        return name
    # Если название уже содержит заглавные буквы в нужных местах (как WhatsApp, Instagram), оставляем как есть
    if any(c.isupper() for c in name[1:]):
        return name
    # Иначе капитализируем первую букву
    return name[0].upper() + name[1:] if len(name) > 0 else name


async def api_get_balance() -> str:
    try:
        result = await api_get({"action": "getBalance"})
        if "ACCESS_BALANCE:" in result:
            return result.split("ACCESS_BALANCE:")[1].strip()
        return "0"
    except Exception:
        return "0"


async def api_get_number(service: str, country: str = "0", operator: str = "any") -> str:
    params = {"action": "getNumber", "service": service, "country": country}
    if operator and operator != "any":
        params["operator"] = operator
    result = await api_get(params)
    return result


async def api_set_status(activation_id: str, status: int) -> str:
    result = await api_get({"action": "setStatus", "id": activation_id, "status": status})
    return result


async def api_get_status(activation_id: str) -> str:
    result = await api_get({"action": "getStatus", "id": activation_id})
    return result


async def api_get_countries() -> list:
    """Получить список стран"""
    try:
        result = await api_get({"action": "getCountries"})
        if result.startswith("["):
            return json.loads(result)
        return []
    except Exception:
        return {}


async def api_get_numbers_status(country: str = "0") -> dict:
    try:
        result = await api_get({"action": "getNumbersStatus", "country": country})
        if result.startswith("{"):
            return json.loads(result)
        return {}
    except Exception:
        return {}


# ==================== STATES ====================
class TopupStates(StatesGroup):
    waiting_amount = State()
    waiting_screenshot = State()
    waiting_crypto_amount = State()


class AdminStates(StatesGroup):
    edit_welcome = State()
    edit_bank_name = State()
    edit_bank_req = State()
    edit_bank_owner = State()
    edit_ref_percent = State()
    edit_min_withdraw = State()
    edit_min_topup = State()
    edit_markup = State()
    edit_support = State()
    edit_cryptobot = State()
    ban_user = State()
    unban_user = State()
    add_balance = State()
    sub_balance = State()
    freeze_user = State()
    unfreeze_user = State()
    find_user = State()
    send_to_user = State()
    set_balance = State()
    edit_user_target = State()
    add_warning = State()
    remove_warning = State()
    add_note = State()
    set_vip = State()
    remove_vip = State()
    add_promo = State()
    delete_promo = State()
    edit_bot_name = State()
    edit_max_numbers = State()
    edit_vip_discount = State()
    edit_default_country = State()
    edit_auto_cancel = State()
    broadcast_old = State()
    set_ref_code = State()
    add_channel_name = State()
    add_channel_url = State()
    edit_subscription_text = State()
    edit_service_price = State()


class BroadcastForm(StatesGroup):
    waiting_for_text = State()
    waiting_for_media = State()
    waiting_for_btn_text = State()
    waiting_for_btn_url = State()


class WithdrawStates(StatesGroup):
    waiting_details = State()


class PromoStates(StatesGroup):
    waiting_code = State()


class RefCodeStates(StatesGroup):
    waiting_new_code = State()


class SupportStates(StatesGroup):
    choosing_reason = State()
    waiting_description = State()
    waiting_screenshot = State()


class SupportStates(StatesGroup):
    choosing_reason = State()
    waiting_description = State()
    waiting_screenshot = State()


class SearchStates(StatesGroup):
    waiting_service_name = State()


class AdminServiceStates(StatesGroup):
    editing_svc_name = State()
    editing_svc_price = State()
    editing_svc_count = State()


# ==================== KEYBOARDS ====================
def main_keyboard(uid: int) -> InlineKeyboardMarkup:
    s = get_settings()
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Купить номер", callback_data="buy_number", icon_custom_emoji_id="5769289093221454192"),
            InlineKeyboardButton(text="Пополнить баланс", callback_data="topup", icon_custom_emoji_id="5904462880941545555"),
        ],
        [
            InlineKeyboardButton(text="История", callback_data="history", icon_custom_emoji_id="5890937706803894250"),
            InlineKeyboardButton(text="Активные номера", callback_data="active_numbers", icon_custom_emoji_id="5884479287171485878"),
        ],
        [
            InlineKeyboardButton(text="Рефералка", callback_data="referral", icon_custom_emoji_id="5870772616305839506"),
            InlineKeyboardButton(text="Поддержка", url=s.get("support_link", SUPPORT_LINK), icon_custom_emoji_id="6039422865189638057"),
        ],
        [
            InlineKeyboardButton(text="Профиль", callback_data="profile", icon_custom_emoji_id="5870994129244131212"),
            InlineKeyboardButton(text="Промокод", callback_data="promo_code", icon_custom_emoji_id="6032644646587338669"),
        ],
    ])


def main_text(uid: int) -> str:
    u = get_user(uid)
    s = get_settings()
    bot_name = s.get("bot_name", "Stuffiny SMS")
    vip_badge = f' {e("6032644646587338669", "🎁")} <b>VIP</b>' if u.get("vip") else ""
    return (
        f'<b>{e("5084754787518383579", "👾")} {bot_name}</b>{vip_badge}\n\n'
        f'<blockquote>'
        f'{e("5904462880941545555", "🪙")} <b>Баланс:</b> {u["balance"]:.2f}₽\n'
        f'{e("5884479287171485878", "📦")} <b>Куплено номеров:</b> {u["activations_count"]}\n'
        f'{e("5870633910337015697", "✅")} <b>Успешных активаций:</b> {u["success_activations"]}\n'
        f'{e("5890848474563352982", "🪙")} <b>Потрачено:</b> {u["total_spent"]:.2f}₽'
        f'</blockquote>'
    )


def admin_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Статистика", callback_data="admin_stats", icon_custom_emoji_id="5870921681735781843"),
            InlineKeyboardButton(text="Рассылка", callback_data="admin_broadcast", icon_custom_emoji_id="6039422865189638057"),
        ],
        [
            InlineKeyboardButton(text="Пользователи", callback_data="admin_users", icon_custom_emoji_id="5870772616305839506"),
            InlineKeyboardButton(text="Настройки", callback_data="admin_settings", icon_custom_emoji_id="5870982283724328568"),
        ],
        [
            InlineKeyboardButton(text="Заявки пополнения", callback_data="admin_topup_requests", icon_custom_emoji_id="5879814368572478751"),
            InlineKeyboardButton(text="Заявки вывода", callback_data="admin_withdraw_requests", icon_custom_emoji_id="5890848474563352982"),
        ],
        [
            InlineKeyboardButton(text="Баланс API", callback_data="admin_api_balance", icon_custom_emoji_id="5904462880941545555"),
            InlineKeyboardButton(text="Техобслуживание", callback_data="admin_toggle_maintenance", icon_custom_emoji_id="6037249452824072506"),
        ],
        [
            InlineKeyboardButton(text="Найти юзера", callback_data="admin_find_user", icon_custom_emoji_id="5870994129244131212"),
            InlineKeyboardButton(text="Все юзеры", callback_data="admin_all_users", icon_custom_emoji_id="5870772616305839506"),
        ],
        [
            InlineKeyboardButton(text="Выдать баланс", callback_data="admin_give_balance", icon_custom_emoji_id="5879814368572478751"),
            InlineKeyboardButton(text="Снять баланс", callback_data="admin_take_balance", icon_custom_emoji_id="5890848474563352982"),
        ],
        [
            InlineKeyboardButton(text="Заморозить юзера", callback_data="admin_freeze", icon_custom_emoji_id="6037249452824072506"),
            InlineKeyboardButton(text="Разморозить юзера", callback_data="admin_unfreeze", icon_custom_emoji_id="6037496202990194718"),
        ],
        [
            InlineKeyboardButton(text="Забанить юзера", callback_data="admin_ban", icon_custom_emoji_id="5870657884844462243"),
            InlineKeyboardButton(text="Разбанить юзера", callback_data="admin_unban", icon_custom_emoji_id="5870633910337015697"),
        ],
        [
            InlineKeyboardButton(text="Выдать VIP", callback_data="admin_set_vip", icon_custom_emoji_id="6032644646587338669"),
            InlineKeyboardButton(text="Убрать VIP", callback_data="admin_remove_vip", icon_custom_emoji_id="5870657884844462243"),
        ],
        [
            InlineKeyboardButton(text="Предупреждение", callback_data="admin_add_warning", icon_custom_emoji_id="6039422865189638057"),
            InlineKeyboardButton(text="Снять предупр.", callback_data="admin_remove_warning", icon_custom_emoji_id="5870633910337015697"),
        ],
        [
            InlineKeyboardButton(text="Заметка о юзере", callback_data="admin_add_note", icon_custom_emoji_id="5870676941614354370"),
            InlineKeyboardButton(text="Написать юзеру", callback_data="admin_message_user", icon_custom_emoji_id="5870676941614354370"),
        ],
        [
            InlineKeyboardButton(text="Промокоды", callback_data="admin_promo_menu", icon_custom_emoji_id="6032644646587338669"),
            InlineKeyboardButton(text="Установить баланс", callback_data="admin_set_balance", icon_custom_emoji_id="5904462880941545555"),
        ],
        [
            InlineKeyboardButton(text="Стартовое сообщение", callback_data="admin_edit_welcome", icon_custom_emoji_id="5870676941614354370"),
            InlineKeyboardButton(text="Настройки банка", callback_data="admin_bank_settings", icon_custom_emoji_id="5879814368572478751"),
        ],
        [
            InlineKeyboardButton(text="Реф. процент", callback_data="admin_ref_percent", icon_custom_emoji_id="5870772616305839506"),
            InlineKeyboardButton(text="Мин. пополнение", callback_data="admin_min_topup", icon_custom_emoji_id="5904462880941545555"),
        ],
        [
            InlineKeyboardButton(text="Мин. вывод", callback_data="admin_min_withdraw", icon_custom_emoji_id="5890848474563352982"),
            InlineKeyboardButton(text="Наценка на номера", callback_data="admin_markup", icon_custom_emoji_id="5870921681735781843"),
        ],
        [
            InlineKeyboardButton(text="Ссылка поддержки", callback_data="admin_support_link", icon_custom_emoji_id="6039422865189638057"),
            InlineKeyboardButton(text="CryptoBot токен", callback_data="admin_cryptobot_token", icon_custom_emoji_id="5084754787518383579"),
        ],
        [
            InlineKeyboardButton(text="Название бота", callback_data="admin_edit_bot_name", icon_custom_emoji_id="5870676941614354370"),
            InlineKeyboardButton(text="VIP скидка %", callback_data="admin_vip_discount", icon_custom_emoji_id="6032644646587338669"),
        ],
        [
            InlineKeyboardButton(text="Макс. номеров", callback_data="admin_max_numbers", icon_custom_emoji_id="5884479287171485878"),
            InlineKeyboardButton(text="Авто-отмена (мин)", callback_data="admin_auto_cancel", icon_custom_emoji_id="5983150113483134607"),
        ],
        [
            InlineKeyboardButton(text="История транзакций", callback_data="admin_transactions", icon_custom_emoji_id="5890937706803894250"),
            InlineKeyboardButton(text="Топ пользователей", callback_data="admin_top_users", icon_custom_emoji_id="5870930636742595124"),
        ],
        [
            InlineKeyboardButton(text="Список забаненных", callback_data="admin_banned_list", icon_custom_emoji_id="5870657884844462243"),
            InlineKeyboardButton(text="Список замороженных", callback_data="admin_frozen_list", icon_custom_emoji_id="6037249452824072506"),
        ],
        [
            InlineKeyboardButton(text="Список VIP", callback_data="admin_vip_list", icon_custom_emoji_id="6032644646587338669"),
            InlineKeyboardButton(text="Активные номера всех", callback_data="admin_all_active", icon_custom_emoji_id="5884479287171485878"),
        ],
        [
            InlineKeyboardButton(text="Экспорт юзеров", callback_data="admin_export_users", icon_custom_emoji_id="5963103826075456248"),
            InlineKeyboardButton(text="Очистить историю", callback_data="admin_clear_history", icon_custom_emoji_id="5870875489362513438"),
        ],
        [
            InlineKeyboardButton(text="Статус API", callback_data="admin_api_status", icon_custom_emoji_id="5940433880585605708"),
            InlineKeyboardButton(text="Логи активности", callback_data="admin_activity_log", icon_custom_emoji_id="6037397706505195857"),
        ],
        [
            InlineKeyboardButton(text="Избранные сервисы", callback_data="admin_featured_services", icon_custom_emoji_id="5884479287171485878"),
            InlineKeyboardButton(text="Выдать реф код", callback_data="admin_give_ref_code", icon_custom_emoji_id="5769289093221454192"),
        ],
        [
            InlineKeyboardButton(text="Поиск заказа по ID", callback_data="admin_find_order", icon_custom_emoji_id="5884479287171485878"),
            InlineKeyboardButton(text="Все жалобы", callback_data="admin_support_tickets", icon_custom_emoji_id="6039422865189638057"),
        ],
        [
            InlineKeyboardButton(text="Управлять подпиской", callback_data="admin_subscription_channels", icon_custom_emoji_id="6039422865189638057"),
            InlineKeyboardButton(text="Текст подписки", callback_data="admin_subscription_text", icon_custom_emoji_id="5870676941614354370"),
        ],
        [
            InlineKeyboardButton(text="Управлять сервисами", callback_data="admin_manage_services", icon_custom_emoji_id="5884479287171485878"),
            InlineKeyboardButton(text="Установить цены", callback_data="admin_set_service_prices", icon_custom_emoji_id="5904462880941545555"),
        ],
    ])


def get_all_broadcast_users() -> list:
    db = load_db()
    return [int(uid) for uid, u in db["users"].items() if not u.get("banned")]


def _bc_build_reply_markup(btn_text, btn_url, btn_emoji_id):
    if not btn_text and not btn_emoji_id:
        return None
    if not btn_url:
        return None
    text = btn_text or "·"
    btn = InlineKeyboardButton(text=text, url=btn_url)
    if btn_emoji_id:
        btn.icon_custom_emoji_id = btn_emoji_id
    return InlineKeyboardMarkup(inline_keyboard=[[btn]])


# ==================== КОМАНДЫ ====================
@dp.message(CommandStart())
async def cmd_start(message: Message):
    uid = message.from_user.id
    db = load_db()
    u = get_user(uid)
    u["first_name"] = message.from_user.first_name
    u["username"] = message.from_user.username
    u["last_activity"] = datetime.now().isoformat()

    args = message.text.split()
    if len(args) > 1 and u.get("referrer") is None:
        ref_code = args[1]
        custom_codes = db.get("custom_ref_codes", {})
        referrer_uid = None
        
        # Сначала проверяем кастомные коды
        for uid_str, code in custom_codes.items():
            if code == ref_code:
                referrer_uid = int(uid_str)
                if referrer_uid != uid:
                    u["referrer"] = referrer_uid
                    other_data = db["users"].get(uid_str)
                    if other_data and uid not in other_data.get("referrals", []):
                        other_data["referrals"].append(uid)
                        db["users"][uid_str] = other_data
                break
        
        # Если кастомного кода не нашли, проверяем стандартные коды
        if u.get("referrer") is None:
            for other_uid, other_data in db["users"].items():
                if other_data.get("ref_code") == ref_code and int(other_uid) != uid:
                    referrer_uid = int(other_uid)
                    u["referrer"] = referrer_uid
                    if uid not in other_data.get("referrals", []):
                        other_data["referrals"].append(uid)
                    db["users"][other_uid] = other_data
                    break
        
        # Отправляем уведомление рефератору
        if referrer_uid:
            try:
                user_name = message.from_user.username or message.from_user.first_name or f"пользователь {uid}"
                time_now = datetime.now().strftime("%d.%m.%Y %H:%M")
                await bot.send_message(
                    referrer_uid,
                    f'{e("5870633910337015697", "✅")} <b>💥 НОВЫЙ РЕФЕРАЛ ПРИСОЕДИНИЛСЯ! 💥</b>\n\n'
                    f'👤 Пользователь: @{user_name}\n'
                    f'🕐 Время: {time_now}\n\n'
                    f'<blockquote>'
                    f'{e("5904462880941545555", "🪙")} <i>Начинайте получать комиссию с его пополнений!</i>'
                    f'</blockquote>',
                    parse_mode="HTML"
                )
            except Exception:
                pass

    db["users"][str(uid)] = u
    save_db(db)
    log_activity(uid, "start")

    s = get_settings()
    if s.get("maintenance") and not is_admin(uid):
        await message.answer(
            f'<blockquote>{e("6037249452824072506", "🔒")} <b>Бот на техническом обслуживании.</b>\nПожалуйста, попробуйте позже.</blockquote>'
        )
        return

    # Проверяем подписку
    if await show_subscription_required(message):
        return

    welcome = s.get("welcome_text") or main_text(uid)
    await message.answer(welcome, reply_markup=main_keyboard(uid))


@dp.callback_query(F.data == "check_subscription")
async def cb_check_subscription(cb: CallbackQuery):
    """Проверка подписки пользователя"""
    uid = cb.from_user.id
    
    if await check_subscription(uid):
        await cb.answer("✅ Спасибо за подписку!", show_alert=False)
        welcome = get_settings().get("welcome_text") or main_text(uid)
        await cb.message.edit_text(welcome, reply_markup=main_keyboard(uid))
    else:
        await cb.answer("❌ Вы не подписаны на все каналы", show_alert=True)


@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        f'{e("5870982283724328568", "⚙️")} <b>Панель администратора</b>\n\n'
        f'<blockquote>Добро пожаловать в панель управления ботом.</blockquote>',
        reply_markup=admin_main_keyboard()
    )


@dp.message(Command("balance"))
async def cmd_balance(message: Message):
    uid = message.from_user.id
    u = get_user(uid)
    await message.answer(
        f'<blockquote>{e("5904462880941545555", "🪙")} <b>Ваш баланс: {u["balance"]:.2f}₽</b></blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Пополнить", callback_data="topup", icon_custom_emoji_id="5904462880941545555")]
        ])
    )


@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    uid = message.from_user.id
    u = get_user(uid)
    joined = u.get("joined_at", "")[:10]
    username = f"@{u['username']}" if u.get("username") else "—"
    vip_badge = f' {e("6032644646587338669", "🎁")} VIP' if u.get("vip") else ""
    text = (
        f'{e("5870994129244131212", "👤")} <b>Профиль</b>{vip_badge}\n\n'
        f'<blockquote>'
        f'<b>ID:</b> <code>{uid}</code>\n'
        f'<b>Юзернейм:</b> {username}\n'
        f'<b>Имя:</b> {u.get("first_name", "—")}\n'
        f'<b>В боте с:</b> {joined}'
        f'</blockquote>\n\n'
        f'{e("5904462880941545555", "🪙")} <b>Баланс:</b> {u["balance"]:.2f}₽\n'
        f'{e("5884479287171485878", "📦")} <b>Куплено номеров:</b> {u["activations_count"]}\n'
        f'{e("5870633910337015697", "✅")} <b>Успешных:</b> {u["success_activations"]}'
    )
    await message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◁ Назад", callback_data="main_menu")]
        ])
    )


# ==================== ГЛАВНОЕ МЕНЮ ====================
@dp.callback_query(F.data == "main_menu")
async def cb_main_menu(cb: CallbackQuery):
    uid = cb.from_user.id
    u = get_user(uid)
    if u.get("frozen"):
        await cb.answer("Ваш аккаунт заморожен", show_alert=True)
        return
    if u.get("banned"):
        await cb.answer("Ваш аккаунт заблокирован", show_alert=True)
        return
    try:
        await cb.message.edit_text(main_text(uid), reply_markup=main_keyboard(uid))
    except Exception:
        await cb.message.answer(main_text(uid), reply_markup=main_keyboard(uid))
    await cb.answer()


# ==================== ПРОФИЛЬ ====================
@dp.callback_query(F.data == "profile")
async def cb_profile(cb: CallbackQuery):
    uid = cb.from_user.id
    u = get_user(uid)
    joined = u.get("joined_at", "")[:10]
    username = f"@{u['username']}" if u.get("username") else "—"
    s = get_settings()
    min_w = s.get("min_withdraw", 500)
    vip_badge = f' {e("6032644646587338669", "🎁")} VIP' if u.get("vip") else ""
    warn_count = u.get("warnings_count", 0)
    text = (
        f'{e("5870994129244131212", "👤")} <b>Профиль</b>{vip_badge}\n\n'
        f'<blockquote>'
        f'<b>ID:</b> <code>{uid}</code>\n'
        f'<b>Юзернейм:</b> {username}\n'
        f'<b>Имя:</b> {u.get("first_name", "—")}\n'
        f'<b>В боте с:</b> {joined}'
        f'</blockquote>\n\n'
        f'{e("5904462880941545555", "🪙")} <b>Баланс:</b> {u["balance"]:.2f}₽\n'
        f'{e("5870772616305839506", "👥")} <b>Рефералов:</b> {len(u.get("referrals", []))}\n'
        f'{e("5890848474563352982", "🪙")} <b>Заработано с рефок:</b> {u.get("ref_balance", 0):.2f}₽\n'
        f'{e("5884479287171485878", "📦")} <b>Куплено номеров:</b> {u["activations_count"]}\n'
        f'{e("5870633910337015697", "✅")} <b>Успешных:</b> {u["success_activations"]}\n'
        f'{e("5890848474563352982", "🪙")} <b>Потрачено:</b> {u["total_spent"]:.2f}₽\n'
        f'{e("5879814368572478751", "🏧")} <b>Пополнено:</b> {u.get("total_topup", 0):.2f}₽'
        + (f'\n{e("6039422865189638057", "📣")} <b>Предупреждений:</b> {warn_count}' if warn_count > 0 else '')
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Вывести", callback_data="withdraw", icon_custom_emoji_id="5890848474563352982")],
        [InlineKeyboardButton(text="Поддержка", callback_data="support", icon_custom_emoji_id="6039422865189638057")],
        [InlineKeyboardButton(text="Назад", callback_data="main_menu")],
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()



@dp.message(Command("clear"))
async def cmd_clear(message: Message):
    """Очищает весь чат кроме команды /start"""
    uid = message.from_user.id
    try:
        # Удаляем сообщение с командой /clear
        await message.delete()
    except:
        pass
    
    # Отправляем чистое главное меню
    u = get_user(uid)
    await message.answer(
        main_text(uid),
        reply_markup=main_keyboard(uid)
    )



# ==================== ПОПОЛНЕНИЕ ====================
@dp.callback_query(F.data == "topup")
async def cb_topup(cb: CallbackQuery):
    uid = cb.from_user.id
    u = get_user(uid)
    s = get_settings()
    text = (
        f'{e("5769126056262898415", "👛")} <b>Пополнение баланса</b>\n\n'
        f'<blockquote>{e("5904462880941545555", "🪙")} <b>Ваш баланс:</b> {u["balance"]:.2f}₽</blockquote>\n\n'
        f'{e("5904462880941545555", "🪙")} <b>Выберите способ пополнения:</b>'
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Прямой перевод", callback_data="topup_bank", icon_custom_emoji_id="5879814368572478751"),
            InlineKeyboardButton(text="CryptoBot", callback_data="topup_crypto", icon_custom_emoji_id="5084754787518383579"),
        ],
        [InlineKeyboardButton(text="Назад", callback_data="main_menu")],
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@dp.callback_query(F.data == "topup_bank")
async def cb_topup_bank(cb: CallbackQuery, state: FSMContext):
    s = get_settings()
    await state.set_state(TopupStates.waiting_amount)
    await state.update_data(topup_method="bank", msg_id=cb.message.message_id)
    await cb.message.edit_text(
        f'{e("5904462880941545555", "🪙")} <b>Введи сумму пополнения</b>\n\n'
        f'<blockquote>Минимальная сумма: <b>{s["min_topup"]}₽</b>\nВведите число:</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="topup")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "topup_crypto")
async def cb_topup_crypto(cb: CallbackQuery, state: FSMContext):
    s = get_settings()
    if not s.get("cryptobot_token"):
        await cb.answer("CryptoBot не настроен. Обратитесь в поддержку.", show_alert=True)
        return
    await state.set_state(TopupStates.waiting_crypto_amount)
    await cb.message.edit_text(
        f'{e("5084754787518383579", "🤖")} <b>Пополнение через CryptoBot</b>\n\n'
        f'<blockquote>Минимальная сумма: <b>{s["min_topup"]}₽</b>\nВведите сумму:</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="topup")]
        ])
    )
    await cb.answer()


@dp.message(TopupStates.waiting_amount)
async def process_topup_amount(message: Message, state: FSMContext):
    s = get_settings()
    try:
        amount = float(message.text.replace(",", "."))
        if amount < s["min_topup"]:
            await message.answer(
                f'<blockquote>{e("5870657884844462243", "❌")} <b>Минимальная сумма: {s["min_topup"]}₽</b>\nВведите другую сумму:</blockquote>',
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◁ Назад", callback_data="topup")]
                ])
            )
            return
    except ValueError:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} <b>Введите корректное число</b></blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="topup")]
            ])
        )
        return

    await state.update_data(topup_amount=amount)
    await state.set_state(TopupStates.waiting_screenshot)

    text = (
        f'{e("5884479287171485878", "📦")} <b>Реквизиты для оплаты</b>\n\n'
        f'<blockquote>'
        f'<b>Банк:</b> {s["bank_name"]}\n'
        f'<b>Реквизиты:</b> <code>{s["bank_requisites"]}</code>\n'
        f'<b>Получатель:</b> {s["bank_owner"]}\n'
        f'{e("5904462880941545555", "🪙")} <b>Сумма к оплате:</b> {amount:.2f}₽'
        f'</blockquote>\n\n'
        f'{e("6035128606563241721", "🖼")} <b>После оплаты пришли скриншот чека</b>'
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="topup")]
    ])
    await message.answer(text, reply_markup=kb)


@dp.message(TopupStates.waiting_screenshot, F.photo)
async def process_topup_screenshot(message: Message, state: FSMContext):
    data = await state.get_data()
    amount = data.get("topup_amount", 0)
    uid = message.from_user.id
    u = get_user(uid)

    db = load_db()
    req_id = f"topup_{uid}_{int(time.time())}"
    if "topup_requests" not in db:
        db["topup_requests"] = {}
    db["topup_requests"][req_id] = {
        "uid": uid,
        "amount": amount,
        "photo_id": message.photo[-1].file_id,
        "status": "pending",
        "date": datetime.now().isoformat(),
        "username": u.get("username"),
        "first_name": u.get("first_name"),
    }
    save_db(db)
    await state.clear()

    await message.answer(
        f'{e("5983150113483134607", "⏰")} <b>Заявка отправлена на проверку</b>\n\n'
        f'<blockquote>'
        f'{e("5904462880941545555", "🪙")} <b>Сумма:</b> {amount:.2f}₽\n'
        f'Ожидайте подтверждения от администратора.'
        f'</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Главная", callback_data="main_menu")]
        ])
    )

    display = get_user_display(u)
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_photo(
                admin_id,
                message.photo[-1].file_id,
                caption=(
                    f'{e("5879814368572478751", "🏧")} <b>Заявка на пополнение</b>\n\n'
                    f'<b>От:</b> {display} (<code>{uid}</code>)\n'
                    f'<b>Сумма:</b> {amount:.2f}₽\n'
                    f'<b>ID заявки:</b> <code>{req_id}</code>'
                ),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="Одобрить", callback_data=f"approve_topup_{req_id}", icon_custom_emoji_id="5870633910337015697"),
                        InlineKeyboardButton(text="Отклонить", callback_data=f"reject_topup_{req_id}", icon_custom_emoji_id="5870657884844462243"),
                    ]
                ])
            )
        except Exception:
            pass


@dp.message(TopupStates.waiting_screenshot)
async def process_topup_no_photo(message: Message):
    await message.answer(
        f'<blockquote>{e("6035128606563241721", "🖼")} <b>Пожалуйста, отправьте скриншот чека (фото)</b></blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◁ Назад", callback_data="topup")]
        ])
    )


@dp.message(TopupStates.waiting_crypto_amount)
async def process_topup_crypto_amount(message: Message, state: FSMContext):
    s = get_settings()
    try:
        amount = float(message.text.replace(",", "."))
        if amount < s["min_topup"]:
            await message.answer(
                f'<blockquote>{e("5870657884844462243", "❌")} Минимальная сумма: {s["min_topup"]}₽</blockquote>',
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◁ Назад", callback_data="topup")]
                ])
            )
            return
    except ValueError:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Введите число</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="topup")]
            ])
        )
        return
    await state.clear()
    await message.answer(
        f'<blockquote>{e("5084754787518383579", "🤖")} <b>CryptoBot</b>\n\nФункция в разработке. Настройте токен CryptoBot в админ панели.</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="topup")]
        ])
    )


# ==================== ОБРАБОТКА ЗАЯВОК ПОПОЛНЕНИЯ ====================
@dp.callback_query(F.data.startswith("approve_topup_"))
async def approve_topup(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    req_id = cb.data.replace("approve_topup_", "")
    db = load_db()
    req = db.get("topup_requests", {}).get(req_id)
    if not req or req["status"] != "pending":
        await cb.answer("Заявка уже обработана", show_alert=True)
        return
    req["status"] = "approved"
    req["approved_by"] = cb.from_user.id
    req["approved_at"] = datetime.now().isoformat()
    db["topup_requests"][req_id] = req
    uid = req["uid"]
    amount = req["amount"]
    u = get_user(uid)
    u["balance"] = round(u["balance"] + amount, 2)
    u["total_topup"] = round(u.get("total_topup", 0) + amount, 2)

    ref_uid = u.get("referrer")
    if ref_uid:
        s = get_settings()
        ref_bonus = round(amount * s["ref_percent"] / 100, 2)
        ref_user = get_user(ref_uid)
        ref_user["balance"] = round(ref_user["balance"] + ref_bonus, 2)
        ref_user["ref_balance"] = round(ref_user.get("ref_balance", 0) + ref_bonus, 2)
        save_user(ref_uid, ref_user)
        add_transaction(ref_uid, ref_bonus, "ref_bonus", f"Реф. бонус от {uid}")
        try:
            await bot.send_message(
                ref_uid,
                f'{e("6041731551845159060", "🎉")} <b>Реферальный бонус +{ref_bonus}₽</b>\n\n'
                f'<blockquote>Ваш реферал пополнил баланс. Вы получили {ref_bonus}₽!</blockquote>'
            )
        except Exception:
            pass

    save_user(uid, u)
    add_transaction(uid, amount, "topup", "Пополнение через банк")
    db["users"][str(uid)] = u
    save_db(db)

    try:
        await bot.send_message(
            uid,
            f'{e("5870633910337015697", "✅")} <b>Баланс пополнен на {amount:.2f}₽</b>\n\n'
            f'<blockquote>{e("5904462880941545555", "🪙")} <b>Текущий баланс:</b> {u["balance"]:.2f}₽</blockquote>'
        )
    except Exception:
        pass

    try:
        await cb.message.edit_caption(
            cb.message.caption + f'\n\n{e("5870633910337015697", "✅")} <b>Одобрено</b>',
            reply_markup=None
        )
    except Exception:
        pass
    await cb.answer("Одобрено ✅")


@dp.callback_query(F.data.startswith("reject_topup_"))
async def reject_topup(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    req_id = cb.data.replace("reject_topup_", "")
    db = load_db()
    req = db.get("topup_requests", {}).get(req_id)
    if not req or req["status"] != "pending":
        await cb.answer("Заявка уже обработана", show_alert=True)
        return
    req["status"] = "rejected"
    req["rejected_by"] = cb.from_user.id
    req["rejected_at"] = datetime.now().isoformat()
    db["topup_requests"][req_id] = req
    save_db(db)

    try:
        await bot.send_message(
            req["uid"],
            f'{e("5870657884844462243", "❌")} <b>Заявка на пополнение отклонена</b>\n\n'
            f'<blockquote>Если вы считаете это ошибкой — обратитесь в поддержку.</blockquote>'
        )
    except Exception:
        pass

    try:
        await cb.message.edit_caption(
            cb.message.caption + f'\n\n{e("5870657884844462243", "❌")} <b>Отклонено</b>',
            reply_markup=None
        )
    except Exception:
        pass
    await cb.answer("Отклонено ❌")


# ==================== ВЫВОД СРЕДСТВ ====================
@dp.callback_query(F.data == "withdraw")
async def cb_withdraw(cb: CallbackQuery, state: FSMContext):
    uid = cb.from_user.id
    u = get_user(uid)
    s = get_settings()
    min_w = s.get("min_withdraw", 500)
    if u["balance"] < min_w:
        await cb.answer(f"Минимальная сумма вывода: {min_w}₽\nВаш баланс: {u['balance']:.2f}₽", show_alert=True)
        return
    await state.set_state(WithdrawStates.waiting_details)
    await cb.message.edit_text(
        f'{e("5890848474563352982", "🪙")} <b>Вывод средств</b>\n\n'
        f'<blockquote>'
        f'{e("5904462880941545555", "🪙")} <b>Доступно:</b> {u["balance"]:.2f}₽\n'
        f'Введите реквизиты и сумму для вывода:'
        f'</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="profile")]
        ])
    )
    await cb.answer()


@dp.message(WithdrawStates.waiting_details)
async def process_withdraw(message: Message, state: FSMContext):
    uid = message.from_user.id
    u = get_user(uid)
    await state.clear()

    db = load_db()
    req_id = f"withdraw_{uid}_{int(time.time())}"
    if "withdraw_requests" not in db:
        db["withdraw_requests"] = {}
    db["withdraw_requests"][req_id] = {
        "uid": uid,
        "details": message.text,
        "amount": u["balance"],
        "status": "pending",
        "date": datetime.now().isoformat(),
        "username": u.get("username"),
    }
    save_db(db)

    await message.answer(
        f'{e("5983150113483134607", "⏰")} <b>Заявка на вывод отправлена</b>\n\n'
        f'<blockquote>Сумма: <b>{u["balance"]:.2f}₽</b>\nОжидайте подтверждения.</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Главная", callback_data="main_menu")]
        ])
    )

    for admin_id in ADMIN_IDS:
        try:
            display = get_user_display(u)
            await bot.send_message(
                admin_id,
                f'{e("5890848474563352982", "🪙")} <b>Заявка на вывод</b>\n\n'
                f'<b>От:</b> {display} (<code>{uid}</code>)\n'
                f'<b>Сумма:</b> {u["balance"]:.2f}₽\n'
                f'<b>Реквизиты:</b> {message.text}\n'
                f'<b>ID:</b> <code>{req_id}</code>',
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="Одобрить", callback_data=f"approve_withdraw_{req_id}", icon_custom_emoji_id="5870633910337015697"),
                        InlineKeyboardButton(text="Отклонить", callback_data=f"reject_withdraw_{req_id}", icon_custom_emoji_id="5870657884844462243"),
                    ]
                ])
            )
        except Exception:
            pass


@dp.callback_query(F.data.startswith("approve_withdraw_"))
async def approve_withdraw(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    req_id = cb.data.replace("approve_withdraw_", "")
    db = load_db()
    req = db.get("withdraw_requests", {}).get(req_id)
    if not req or req["status"] != "pending":
        await cb.answer("Уже обработано", show_alert=True)
        return
    req["status"] = "approved"
    req["approved_by"] = cb.from_user.id
    req["approved_at"] = datetime.now().isoformat()
    db["withdraw_requests"][req_id] = req
    uid = req["uid"]
    amount = req["amount"]
    u = get_user(uid)
    u["balance"] = 0
    save_user(uid, u)
    add_transaction(uid, amount, "withdraw", "Вывод средств")
    save_db(db)
    try:
        await bot.send_message(
            uid,
            f'{e("5870633910337015697", "✅")} <b>Вывод {amount:.2f}₽ одобрен</b>\n\n'
            f'<blockquote>Средства будут переведены на указанные реквизиты.</blockquote>'
        )
    except Exception:
        pass
    try:
        await cb.message.edit_text(cb.message.text + f'\n\n{e("5870633910337015697", "✅")} <b>Одобрено</b>', reply_markup=None)
    except Exception:
        pass
    await cb.answer("Одобрено")


@dp.callback_query(F.data.startswith("reject_withdraw_"))
async def reject_withdraw(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    req_id = cb.data.replace("reject_withdraw_", "")
    db = load_db()
    req = db.get("withdraw_requests", {}).get(req_id)
    if not req or req["status"] != "pending":
        await cb.answer("Уже обработано", show_alert=True)
        return
    req["status"] = "rejected"
    db["withdraw_requests"][req_id] = req
    save_db(db)
    try:
        await bot.send_message(
            req["uid"],
            f'{e("5870657884844462243", "❌")} <b>Заявка на вывод отклонена</b>\n\n'
            f'<blockquote>Обратитесь в поддержку для уточнения причин.</blockquote>'
        )
    except Exception:
        pass
    try:
        await cb.message.edit_text(cb.message.text + f'\n\n{e("5870657884844462243", "❌")} <b>Отклонено</b>', reply_markup=None)
    except Exception:
        pass
    await cb.answer("Отклонено")


# ==================== ПРОМОКОДЫ ====================
@dp.callback_query(F.data == "promo_code")
async def cb_promo_code(cb: CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_code)
    await cb.message.edit_text(
        f'{e("6032644646587338669", "🎁")} <b>Промокод</b>\n\n'
        f'<blockquote>Введите промокод для получения бонуса:</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="main_menu")]
        ])
    )
    await cb.answer()


@dp.message(PromoStates.waiting_code)
async def process_promo_code(message: Message, state: FSMContext):
    uid = message.from_user.id
    code = message.text.strip().upper()
    db = load_db()
    promos = db.get("promo_codes", {})
    u = get_user(uid)

    if code not in promos:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} <b>Промокод не найден или уже недействителен</b></blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="main_menu")]
            ])
        )
        await state.clear()
        return

    promo = promos[code]
    if promo.get("uses", 0) <= 0:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} <b>Промокод исчерпан</b></blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="main_menu")]
            ])
        )
        await state.clear()
        return

    if code in u.get("promo_used", []):
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} <b>Вы уже использовали этот промокод</b></blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="main_menu")]
            ])
        )
        await state.clear()
        return

    amount = promo["amount"]
    u["balance"] = round(u["balance"] + amount, 2)
    if "promo_used" not in u:
        u["promo_used"] = []
    u["promo_used"].append(code)
    save_user(uid, u)
    promo["uses"] -= 1
    if promo["uses"] <= 0:
        del promos[code]
    else:
        promos[code] = promo
    db["promo_codes"] = promos
    save_db(db)
    add_transaction(uid, amount, "promo", f"Промокод {code}")

    await message.answer(
        f'{e("6041731551845159060", "🎉")} <b>Промокод активирован!</b>\n\n'
        f'<blockquote>{e("5904462880941545555", "🪙")} <b>На баланс начислено: {amount:.2f}₽</b>\nТекущий баланс: {u["balance"]:.2f}₽</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Главная", callback_data="main_menu")]
        ])
    )
    await state.clear()


# ==================== ПОКУПКА НОМЕРА ====================
@dp.callback_query(F.data == "buy_number")
async def cb_buy_number(cb: CallbackQuery):
    uid = cb.from_user.id
    u = get_user(uid)
    if u.get("frozen"):
        await cb.answer("Аккаунт заморожен", show_alert=True)
        return
    if u.get("banned"):
        await cb.answer("Аккаунт заблокирован", show_alert=True)
        return

    s = get_settings()
    max_active = s.get("max_active_numbers", 10)
    if len(u.get("active_numbers", [])) >= max_active:
        await cb.answer(f"Максимум активных номеров: {max_active}", show_alert=True)
        return

    msg = await cb.message.edit_text(
        f'{e("5345906554510012647", "🔄")} <b>Загружаю сервисы...</b>',
        reply_markup=None
    )

    db = load_db()
    old_count = len(db.get("cached_services", {}))
    
    country = s.get("default_country", "187")
    services_data = await api_get_services(country)
    
    if not services_data:
        await msg.edit_text(
            f'<blockquote>{e("5870657884844462243", "❌")} <b>Не удалось загрузить сервисы</b>\nПроверьте API ключ или попробуйте позже.</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Повторить", callback_data="buy_number", icon_custom_emoji_id="5345906554510012647")],
                [InlineKeyboardButton(text="Назад", callback_data="main_menu")]
            ])
        )
        return

    # Конвертируем список в словарь если нужно
    if isinstance(services_data, list):
        services_dict = {}
        for svc in services_data:
            if isinstance(svc, dict):
                code = svc.get("code", svc.get("id"))
                services_dict[code] = svc
        country_data = services_dict
    else:
        country_data = services_data
    
    services = []
    markup_price = s.get("price_markup", 0)
    vip_discount = s.get("vip_discount", 0) if u.get("vip") else 0
    
    # Загружаем кастомные цены
    custom_prices = db.get("custom_service_prices", {})
    
    # Процесс загрузки
    new_count = 0
    for service_code, service_data in country_data.items():
        if isinstance(service_data, dict):
            service_name = capitalize_service_name(service_data.get("name", service_code))
            
            # Проверяем есть ли кастомная цена
            if service_code in custom_prices:
                cost = float(custom_prices[service_code].get("price", 0))
            else:
                cost = float(service_data.get("price", service_data.get("cost", 0)))
            
            count = int(service_data.get("count", 0))
            
            # Показываем все сервисы, даже если count=0 (для страны "0" count всегда 0)
            final_price = round(cost + markup_price - (cost * vip_discount / 100), 2) if cost > 0 else 0
            if final_price < 0:
                final_price = 0
            services.append((service_code, service_name, final_price, count))
            new_count += 1

    # Сохраняем в кэш
    db["cached_services"] = {
        sv[0]: {"name": sv[1], "price": sv[2], "count": sv[3]} 
        for sv in services
    }
    save_db(db)
    
    # Сортируем по приоритету и названию
    services.sort(key=lambda x: (get_service_priority(x[0]), x[1]))

    # Показываем сервисы сразу
    await show_services_page(msg, uid, services, 0)
    await cb.answer()


async def show_services_page(msg, uid: int, services: list, page: int):
    per_page = 9
    total_pages = max(1, (len(services) + per_page - 1) // per_page)
    start = page * per_page
    end = start + per_page
    page_services = services[start:end]

    rows = []
    row = []
    for i, svc in enumerate(page_services):
        # Формат: (code, name, price, count) или (code, price, count, name) для старой версии
        if len(svc) == 4:
            # Проверяем типы - если второе значение строка, это новый формат
            if isinstance(svc[1], str):
                service, service_name, price, count = svc
            else:
                service, price, count, service_name = svc
        else:
            service = svc[0]
            price = svc[1] if len(svc) > 1 else 0
            count = svc[2] if len(svc) > 2 else 0
            service_name = svc[3] if len(svc) > 3 else service
        
        btn = InlineKeyboardButton(
            text=f"{service_name}",
            callback_data=f"select_service_{service}_{page}"
        )
        row.append(btn)
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton(text="Поиск сервиса", callback_data=f"search_service_{page}", icon_custom_emoji_id="5870676941614354370")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◁", callback_data=f"services_page_{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="▷", callback_data=f"services_page_{page + 1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="◁ Назад", callback_data="main_menu")])

    await msg.edit_text(
        f'{e("5884479287171485878", "📦")} <b>Выберите сервис</b>\n\n'
        f'<blockquote>Всего сервисов: <b>{len(services)}</b> | Страница {page + 1}/{total_pages}</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )


@dp.callback_query(F.data.startswith("services_page_"))
async def cb_services_page(cb: CallbackQuery):
    page = int(cb.data.replace("services_page_", ""))
    db = load_db()
    cached = db.get("cached_services", {})
    # Формат: (code, name, price, count)
    services = [(k, capitalize_service_name(v.get("name", k)), v.get("price", 0), v.get("count", 0)) for k, v in cached.items()]
    services.sort(key=lambda x: (get_service_priority(x[0]), x[1]))  # Sort by priority then by name
    await show_services_page(cb.message, cb.from_user.id, services, page)
    await cb.answer()


@dp.callback_query(F.data.startswith("select_service_"))
async def cb_select_service(cb: CallbackQuery):
    parts = cb.data.split("_")
    service = parts[2]
    page = int(parts[3]) if len(parts) > 3 else 0
    uid = cb.from_user.id
    u = get_user(uid)
    db = load_db()
    cached = db.get("cached_services", {})
    svc = cached.get(service, {})
    price = svc.get("price", 0)
    count = svc.get("count", 0)
    service_name = svc.get("name", service)

    if u["balance"] < price:
        await cb.answer(f"Недостаточно средств!\nНужно: {price}₽\nВаш баланс: {u['balance']:.2f}₽", show_alert=True)
        return

    text = (
        f'{e("5884479287171485878", "📦")} <b>Сервис:</b> {service_name}\n'
        f'<blockquote><code>{service}</code></blockquote>\n'
        f'<blockquote>'
        f'{e("5904462880941545555", "🪙")} <b>Цена:</b> {price}₽\n'
        f'{e("5870633910337015697", "✅")} <b>Доступно номеров:</b> {count}\n'
        f'{e("5769126056262898415", "👛")} <b>Ваш баланс:</b> {u["balance"]:.2f}₽'
        f'</blockquote>'
    )
    buy_text = f"Купить номер для {service_name[:20]}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=buy_text, callback_data=f"confirm_buy_{service}", icon_custom_emoji_id="5879814368572478751")],
        [InlineKeyboardButton(text="◁ Назад", callback_data=f"services_page_{page}")],
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@dp.callback_query(F.data.startswith("confirm_buy_"))
async def cb_confirm_buy(cb: CallbackQuery):
    service = cb.data.replace("confirm_buy_", "")
    uid = cb.from_user.id
    u = get_user(uid)
    db = load_db()
    cached = db.get("cached_services", {})
    svc = cached.get(service, {})
    price = svc.get("price", 0)

    if u["balance"] < price:
        await cb.answer("Недостаточно средств", show_alert=True)
        return

    s = get_settings()
    country = s.get("default_country", "0")
    result = await api_get_number(service, country)

    if "ACCESS_NUMBER:" in result:
        parts_r = result.split(":")
        act_id = parts_r[1]
        number = parts_r[2]
        
        # Генерируем ID заказа
        import uuid
        order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"

        u["balance"] = round(u["balance"] - price, 2)
        u["total_spent"] = round(u.get("total_spent", 0) + price, 2)
        u["activations_count"] = u.get("activations_count", 0) + 1
        if "active_numbers" not in u:
            u["active_numbers"] = []
        
        order_data = {
            "id": act_id,
            "number": number,
            "service": service,
            "price": price,
            "date": datetime.now().isoformat(),
            "order_id": order_id,
            "user_id": uid,
            "status": "active",
        }
        u["active_numbers"].append(order_data)
        
        # Сохраняем в БД
        db = load_db()
        if "orders" not in db:
            db["orders"] = {}
        db["orders"][order_id] = order_data
        save_db(db)
        
        save_user(uid, u)
        add_transaction(uid, price, "buy", f"Номер для {service} ({order_id})")
        log_activity(uid, f"bought_number:{service}:order_id={order_id}")

        await cb.message.edit_text(
            f'{e("5870633910337015697", "✅")} <b>Номер получен!</b>\n\n'
            f'<blockquote>'
            f'<b>ID заказа:</b> <code>{order_id}</code>\n'
            f'<b>Номер:</b> <code>{number}</code>\n'
            f'<b>Сервис:</b> {service}\n'
            f'{e("5983150113483134607", "⏰")} <b>Ожидание SMS...</b>'
            f'</blockquote>\n\n'
            f'{e("5904462880941545555", "🪙")} <b>Списано:</b> {price}₽\n'
            f'{e("5769126056262898415", "👛")} <b>Остаток:</b> {u["balance"]:.2f}₽',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="Проверить SMS", callback_data=f"num_detail_{act_id}", icon_custom_emoji_id="5345906554510012647"),
                    InlineKeyboardButton(text="Отменить", callback_data=f"num_cancel_{act_id}", icon_custom_emoji_id="5870657884844462243"),
                ],
                [InlineKeyboardButton(text="Главная", callback_data="main_menu")],
            ])
        )
    else:
        error_map = {
            "NO_NUMBERS": "Нет доступных номеров для этого сервиса",
            "NO_BALANCE": "Недостаточно баланса на API",
            "BAD_SERVICE": "Неверный идентификатор сервиса",
            "BAD_KEY": "Неверный API ключ",
            "ERROR_SQL": "Ошибка сервера SMS",
            "ERROR_TIMEOUT": "Сервис недоступен, попробуйте позже",
        }
        err_text = error_map.get(result, f"Ошибка: {result}")
        await cb.message.edit_text(
            f'<blockquote>{e("5870657884844462243", "❌")} <b>{err_text}</b></blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Попробовать снова", callback_data="buy_number", icon_custom_emoji_id="5345906554510012647")],
                [InlineKeyboardButton(text="Назад", callback_data="main_menu")]
            ])
        )
    await cb.answer()


# ==================== АКТИВНЫЕ НОМЕРА ====================
@dp.callback_query(F.data == "active_numbers")
async def cb_active_numbers(cb: CallbackQuery):
    uid = cb.from_user.id
    u = get_user(uid)
    active = u.get("active_numbers", [])
    if not active:
        await cb.message.edit_text(
            f'<blockquote>{e("5870657884844462243", "❌")} <b>Нет активных номеров</b>\n\nКупите номер для начала работы.</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Купить номер", callback_data="buy_number", icon_custom_emoji_id="5769289093221454192")],
                [InlineKeyboardButton(text="◁ Назад", callback_data="main_menu")]
            ])
        )
    else:
        rows = []
        for num in active:
            rows.append([
                InlineKeyboardButton(
                    text=f"📱 {num['number']} — {num['service']}",
                    callback_data=f"num_detail_{num['id']}"
                )
            ])
        rows.append([InlineKeyboardButton(text="◁Назад", callback_data="main_menu")])
        await cb.message.edit_text(
            f'{e("5884479287171485878", "📦")} <b>Активные номера</b>\n\n'
            f'<blockquote>У вас {len(active)} активных номеров</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
        )
    await cb.answer()


@dp.callback_query(F.data.startswith("num_detail_"))
async def cb_num_detail(cb: CallbackQuery):
    uid = cb.from_user.id
    act_id = cb.data.replace("num_detail_", "")
    u = get_user(uid)
    active = u.get("active_numbers", [])
    num_data = next((n for n in active if str(n["id"]) == act_id), None)
    if not num_data:
        await cb.answer("Номер не найден", show_alert=True)
        return

    status_raw = await api_get_status(act_id)
    code = ""
    if "STATUS_OK:" in status_raw:
        code = status_raw.split("STATUS_OK:")[1].strip()
        text = (
            f'{e("5870633910337015697", "✅")} <b>Код получен!</b>\n\n'
            f'<blockquote>'
            f'<b>Номер:</b> <code>{num_data["number"]}</code>\n'
            f'<b>Сервис:</b> {num_data["service"]}\n'
            f'{e("5940433880585605708", "🔨")} <b>Код:</b> <code>{code}</code>'
            f'</blockquote>'
        )
    elif "STATUS_CANCEL" in status_raw:
        text = (
            f'<blockquote>{e("5870657884844462243", "❌")} <b>Активация отменена</b>\n\n'
            f'<b>Номер:</b> <code>{num_data["number"]}</code></blockquote>'
        )
    elif "STATUS_WAIT_CODE" in status_raw:
        text = (
            f'{e("5983150113483134607", "⏰")} <b>Ожидание SMS</b>\n\n'
            f'<blockquote>'
            f'<b>Номер:</b> <code>{num_data["number"]}</code>\n'
            f'<b>Сервис:</b> {num_data["service"]}\n'
            f'SMS ещё не пришло...'
            f'</blockquote>'
        )
    else:
        text = (
            f'{e("5983150113483134607", "⏰")} <b>Ожидание SMS</b>\n\n'
            f'<blockquote>'
            f'<b>Номер:</b> <code>{num_data["number"]}</code>\n'
            f'<b>Сервис:</b> {num_data["service"]}'
            f'</blockquote>'
        )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Обновить", callback_data=f"num_detail_{act_id}", icon_custom_emoji_id="5345906554510012647"),
            InlineKeyboardButton(text="Завершить", callback_data=f"num_finish_{act_id}", icon_custom_emoji_id="5870633910337015697"),
        ],
        [InlineKeyboardButton(text="Отменить (возврат)", callback_data=f"num_cancel_{act_id}", icon_custom_emoji_id="5870657884844462243")],
        [InlineKeyboardButton(text="◁Назад", callback_data="active_numbers")],
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@dp.callback_query(F.data.startswith("num_finish_"))
async def cb_num_finish(cb: CallbackQuery):
    uid = cb.from_user.id
    act_id = cb.data.replace("num_finish_", "")
    await api_set_status(act_id, 6)
    u = get_user(uid)
    u["active_numbers"] = [n for n in u.get("active_numbers", []) if str(n["id"]) != act_id]
    u["success_activations"] = u.get("success_activations", 0) + 1
    save_user(uid, u)
    await cb.answer("Активация завершена успешно!", show_alert=True)
    await cb_active_numbers(cb)


@dp.callback_query(F.data.startswith("num_cancel_"))
async def cb_num_cancel(cb: CallbackQuery):
    uid = cb.from_user.id
    act_id = cb.data.replace("num_cancel_", "")
    result = await api_set_status(act_id, 8)
    u = get_user(uid)
    num_data = next((n for n in u.get("active_numbers", []) if str(n["id"]) == act_id), None)
    if num_data:
        refund = num_data.get("price", 0)
        u["balance"] = round(u["balance"] + refund, 2)
        u["total_spent"] = round(u.get("total_spent", 0) - refund, 2)
        if u["total_spent"] < 0:
            u["total_spent"] = 0
        add_transaction(uid, refund, "refund", f"Возврат за {num_data['service']}")
    u["active_numbers"] = [n for n in u.get("active_numbers", []) if str(n["id"]) != act_id]
    save_user(uid, u)
    await cb.answer("Отменено, средства возвращены на баланс!", show_alert=True)
    await cb_active_numbers(cb)


# ==================== ПОДДЕРЖКА ====================
@dp.callback_query(F.data == "support")
async def cb_support(cb: CallbackQuery, state: FSMContext):
    """Главное меню поддержки - выбор причины"""
    await state.set_state(SupportStates.choosing_reason)
    text = (
        f'{e("6039422865189638057", "📞")} <b>Техническая поддержка</b>\n\n'
        f'<blockquote>Выберите причину обращения:</blockquote>'
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Проблемы с покупкой", callback_data="sup_reason_purchase")],
        [InlineKeyboardButton(text="Бот не выдал номер", callback_data="sup_reason_no_number")],
        [InlineKeyboardButton(text="Не приходит СМС", callback_data="sup_reason_no_sms")],
        [InlineKeyboardButton(text="Возврат денег", callback_data="sup_reason_refund")],
        [InlineKeyboardButton(text="Другое", callback_data="sup_reason_other")],
        [InlineKeyboardButton(text="◁ Назад", callback_data="profile")],
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@dp.callback_query(F.data.startswith("sup_reason_"))
async def cb_support_reason(cb: CallbackQuery, state: FSMContext):
    """Выбрана причина - просим описание"""
    reason_map = {
        "purchase": "Проблемы с покупкой",
        "no_number": "Бот не выдал номер",
        "no_sms": "Не приходит СМС",
        "refund": "Возврат денег",
        "other": "Другое",
    }
    reason_key = cb.data.replace("sup_reason_", "")
    reason_text = reason_map.get(reason_key, "Другое")
    
    await state.update_data(reason=reason_key, reason_text=reason_text)
    await state.set_state(SupportStates.waiting_description)
    
    text = (
        f'{e("6039422865189638057", "📞")} <b>Поддержка - {reason_text}</b>\n\n'
        f'<blockquote>Опишите подробно вашу проблему:</blockquote>'
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Отмена", callback_data="support")],
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@dp.message(SupportStates.waiting_description)
async def process_support_description(message: Message, state: FSMContext):
    """Получаем описание проблемы"""
    data = await state.get_data()
    await state.update_data(description=message.text)
    await state.set_state(SupportStates.waiting_screenshot)
    
    text = (
        f'{e("6039422865189638057", "📞")} <b>Поддержка - {data["reason_text"]}</b>\n\n'
        f'<blockquote>Предоставьте скрин (опционально).\n'
        f'Отправьте скрин или нажмите кнопку "Пропустить"</blockquote>'
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Пропустить", callback_data="sup_skip_screenshot")],
        [InlineKeyboardButton(text="Отмена", callback_data="support")],
    ])
    await message.answer(text, reply_markup=kb)
    await state.set_state(SupportStates.waiting_screenshot)


@dp.message(SupportStates.waiting_screenshot)
async def process_support_screenshot(message: Message, state: FSMContext):
    """Получаем скрин"""
    if message.photo:
        photo_id = message.photo[-1].file_id
        await state.update_data(screenshot=photo_id)
    
    # Сохраняем тикет в БД
    data = await state.get_data()
    uid = message.from_user.id
    
    import uuid
    ticket_id = f"TKT-{uuid.uuid4().hex[:8].upper()}"
    
    ticket_data = {
        "id": ticket_id,
        "user_id": uid,
        "username": message.from_user.username or f"ID{uid}",
        "reason": data.get("reason", "other"),
        "reason_text": data.get("reason_text", "Другое"),
        "description": data.get("description", ""),
        "screenshot": data.get("screenshot", None),
        "date": datetime.now().isoformat(),
        "status": "open",
        "admin_response": None,
    }
    
    db = load_db()
    if "support_tickets" not in db:
        db["support_tickets"] = {}
    db["support_tickets"][ticket_id] = ticket_data
    save_db(db)
    
    # Уведомляем всех админов о новом тикете
    for admin_id in ADMIN_IDS:
        try:
            admin_text = (
                f'{e("6039422865189638057", "🆘")} <b>НОВЫЙ ТИКЕТ ПОДДЕРЖКИ</b>\n\n'
                f'<blockquote>'
                f'<b>ID тикета:</b> <code>{ticket_id}</code>\n'
                f'<b>От пользователя:</b> @{ticket_data["username"]} (ID: {uid})\n'
                f'<b>Причина:</b> {ticket_data["reason_text"]}\n'
                f'<b>Описание:</b> {ticket_data["description"]}'
                f'</blockquote>'
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Посмотреть тикет", callback_data=f"admin_view_ticket_{ticket_id}")],
                [InlineKeyboardButton(text="Все тикеты", callback_data="admin_support_tickets")],
            ])
            await bot.send_message(admin_id, admin_text, reply_markup=kb)
            
            # Если есть скриншот, отправляем его
            if ticket_data.get("screenshot"):
                await bot.send_photo(admin_id, ticket_data["screenshot"], caption=f"Скриншот к тикету {ticket_id}")
        except Exception as e:
            print(f"Не удалось отправить уведомление админу {admin_id}: {e}")
    
    await state.clear()
    await message.answer(
        f'{e("5870633910337015697", "✅")} <b>Тикет создан!</b>\n\n'
        f'<blockquote>'
        f'<b>ID тикета:</b> <code>{ticket_id}</code>\n'
        f'<b>Статус:</b> Открыт\n'
        f'<b>Наша команда вскоре ответит вам</b>'
        f'</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="В профиль", callback_data="profile")],
            [InlineKeyboardButton(text="Главная", callback_data="main_menu")],
        ])
    )


@dp.callback_query(F.data == "sup_skip_screenshot")
async def cb_skip_screenshot(cb: CallbackQuery, state: FSMContext):
    """Пропускаем скрин"""
    data = await state.get_data()
    uid = cb.from_user.id
    
    import uuid
    ticket_id = f"TKT-{uuid.uuid4().hex[:8].upper()}"
    
    ticket_data = {
        "id": ticket_id,
        "user_id": uid,
        "username": cb.from_user.username or f"ID{uid}",
        "reason": data.get("reason", "other"),
        "reason_text": data.get("reason_text", "Другое"),
        "description": data.get("description", ""),
        "screenshot": None,
        "date": datetime.now().isoformat(),
        "status": "open",
        "admin_response": None,
    }
    
    db = load_db()
    if "support_tickets" not in db:
        db["support_tickets"] = {}
    db["support_tickets"][ticket_id] = ticket_data
    save_db(db)
    
    # Уведомляем всех админов о новом тикете
    for admin_id in ADMIN_IDS:
        try:
            admin_text = (
                f'{e("6039422865189638057", "🆘")} <b>НОВЫЙ ТИКЕТ ПОДДЕРЖКИ</b>\n\n'
                f'<blockquote>'
                f'<b>ID тикета:</b> <code>{ticket_id}</code>\n'
                f'<b>От пользователя:</b> @{ticket_data["username"]} (ID: {uid})\n'
                f'<b>Причина:</b> {ticket_data["reason_text"]}\n'
                f'<b>Описание:</b> {ticket_data["description"]}'
                f'</blockquote>'
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Посмотреть тикет", callback_data=f"admin_view_ticket_{ticket_id}")],
                [InlineKeyboardButton(text="Все тикеты", callback_data="admin_support_tickets")],
            ])
            await bot.send_message(admin_id, admin_text, reply_markup=kb)
        except Exception as e:
            print(f"Не удалось отправить уведомление админу {admin_id}: {e}")
    
    await state.clear()
    await cb.message.edit_text(
        f'{e("5870633910337015697", "✅")} <b>Тикет создан!</b>\n\n'
        f'<blockquote>'
        f'<b>ID тикета:</b> <code>{ticket_id}</code>\n'
        f'<b>Статус:</b> Открыт\n'
        f'<b>Наша команда вскоре ответит вам</b>'
        f'</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="В профиль", callback_data="profile")],
            [InlineKeyboardButton(text="Главная", callback_data="main_menu")],
        ])
    )
    await cb.answer()


# ==================== ИСТОРИЯ ====================
@dp.callback_query(F.data == "history")
async def cb_history(cb: CallbackQuery):
    uid = cb.from_user.id
    db = load_db()
    txs = [t for t in db.get("transactions", []) if t["uid"] == uid]
    txs = txs[-15:]
    if not txs:
        text = (
            f'{e("5890937706803894250", "📅")} <b>История транзакций</b>\n\n'
            f'<blockquote>У вас пока нет транзакций.</blockquote>'
        )
    else:
        lines = [f'{e("5890937706803894250", "📅")} <b>История транзакций</b>\n']
        type_map = {
            "topup": "Пополнение",
            "buy": "Покупка номера",
            "withdraw": "Вывод",
            "ref_bonus": "Реф. бонус",
            "admin_add": "Начисление",
            "refund": "Возврат",
            "promo": "Промокод",
        }
        for t in reversed(txs):
            sign = "+" if t["type"] in ("topup", "ref_bonus", "admin_add", "refund", "promo") else "-"
            type_label = type_map.get(t["type"], t["type"])
            lines.append(f'<code>{t["date"][:10]}</code> {sign}{t["amount"]:.2f}₽ — {type_label}')
        text = "\n".join(lines)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="main_menu")]
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


# ==================== РЕФЕРАЛЬНАЯ СИСТЕМА ====================
@dp.callback_query(F.data == "referral")
async def cb_referral(cb: CallbackQuery):
    uid = cb.from_user.id
    u = get_user(uid)
    s = get_settings()
    me = await bot.get_me()
    
    # Проверяем есть ли кастомный реф код
    db = load_db()
    custom_codes = db.get("custom_ref_codes", {})
    ref_code = custom_codes.get(str(uid), u['ref_code'])
    
    ref_link = f"https://t.me/{me.username}?start={ref_code}"
    text = (
        f'{e("5870772616305839506", "👥")} <b>Реферальная система</b>\n\n'
        f'<blockquote>'
        f'{e("5870772616305839506", "👥")} <b>Приглашено:</b> {len(u.get("referrals", []))}\n'
        f'{e("5904462880941545555", "🪙")} <b>Заработано с рефок:</b> {u.get("ref_balance", 0):.2f}₽\n'
        f'{e("5890848474563352982", "🪙")} <b>Ваш процент:</b> {s["ref_percent"]}%'
        f'</blockquote>\n\n'
        f'{e("6028435952299413210", "ℹ")} За каждое пополнение вашего реферала вы получаете <b>{s["ref_percent"]}%</b> от суммы\n\n'
        f'{e("5769289093221454192", "🔗")} <b>Ваша ссылка:</b>\n<code>{ref_link}</code>'
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Пригласить", url=f"https://t.me/share/url?url={ref_link}&text=💰%20Получай%20SMS%20коды%20БЫСТРО%20и%20ДЁШЕВО!%20🚀%0AПервая%20покупка%20со%20скидкой%20🎁", icon_custom_emoji_id="6039422865189638057"),
        ],
        [InlineKeyboardButton(text="Изменить ссылку", callback_data="change_ref_code")],
        [InlineKeyboardButton(text="◁ Назад", callback_data="main_menu")],
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@dp.callback_query(F.data == "change_ref_code")
async def cb_change_ref_code(cb: CallbackQuery, state: FSMContext):
    uid = cb.from_user.id
    await state.set_state(RefCodeStates.waiting_new_code)
    await cb.message.edit_text(
        f'{e("5769289093221454192", "🔗")} <b>Введите новый код ссылки</b>\n\n'
        f'<blockquote>Можно использовать буквы, цифры, подчёркивания.\nПримеры: my_ref, ref123, mystufiny</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="referral")]
        ])
    )
    await cb.answer()


@dp.message(RefCodeStates.waiting_new_code)
async def msg_set_ref_code(message: Message, state: FSMContext):
    uid = message.from_user.id
    new_code = message.text.strip()
    
    # Валидация кода
    if not new_code or len(new_code) < 3 or len(new_code) > 15:
        await message.answer("❌ Код должен быть от 3 до 15 символов")
        return
    
    if not all(c.isalnum() or c == '_' for c in new_code):
        await message.answer("❌ Используйте только буквы, цифры и подчёркивание")
        return
    
    # Проверяем что кода не существует у другого пользователя
    db = load_db()
    custom_codes = db.get("custom_ref_codes", {})
    
    for existing_uid, existing_code in custom_codes.items():
        if existing_code == new_code and str(uid) != existing_uid:
            await message.answer("❌ Этот код уже используется другим пользователем")
            return
    
    # Сохраняем новый код
    custom_codes[str(uid)] = new_code
    db["custom_ref_codes"] = custom_codes
    save_db(db)
    
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start={new_code}"
    
    await message.answer(
        f'{e("5870633910337015697", "✅")} <b>Ссылка успешно изменена!</b>\n\n'
        f'{e("5769289093221454192", "🔗")} <b>Новая ссылка:</b>\n<code>{ref_link}</code>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад к рефералке", callback_data="referral")],
            [InlineKeyboardButton(text="Главная", callback_data="main_menu")]
        ])
    )
    await state.clear()


# ==================== ПОИСК СЕРВИСОВ ====================
@dp.callback_query(F.data.startswith("search_service_"))
async def cb_search_service(cb: CallbackQuery, state: FSMContext):
    await state.set_state(SearchStates.waiting_service_name)
    await cb.message.edit_text(
        f'{e("5870676941614354370", "🔍")} <b>Поиск сервиса</b>\n\n'
        f'Введите название сервиса для поиска:',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="buy_number")]
        ])
    )
    await cb.answer()


@dp.message(SearchStates.waiting_service_name)
async def process_search_service(message: Message, state: FSMContext):
    search_text = message.text.lower()
    db = load_db()
    
    # Ищем сервисы по названию
    results = []
    
    # Получаем список сервисов
    services = await api_get_services(1)  # country_id=1 (Kazakhstan)
    
    # Конвертируем список в словарь если нужно
    if isinstance(services, list):
        services_dict = {}
        for svc in services:
            if isinstance(svc, dict):
                code = svc.get("code", svc.get("id"))
                services_dict[code] = svc
        services = services_dict
    
    if services and isinstance(services, dict):
        for service_code, service_data in services.items():
            service_name = capitalize_service_name(service_data.get("name", service_code))
            service_name_lower = service_name.lower()
            if search_text in service_name_lower:
                price = service_data.get("price", 0)
                count = service_data.get("count", 0)
                results.append((service_code, service_name, price, count))
    
    if not results:
        await message.answer(
            f'{e("5870676941614354370", "🔍")} <b>Результаты поиска</b>\n\n'
            f'По запросу "<b>{message.text}</b>" ничего не найдено',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="buy_number")]
            ])
        )
        await state.clear()
        return
    
    # Форматируем результаты (максимум 10)
    text = f'{e("5870676941614354370", "🔍")} <b>Результаты поиска</b>\n\n'
    text += f'Найдено: <b>{len(results)}</b> сервис(ов)\n\n'
    
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for code, name, price, count in results[:10]:
        if count > 0:
            kb.inline_keyboard.append([
                InlineKeyboardButton(text=f"{name} — {price}₽", callback_data=f"service_detail_{code}")
            ])
    
    kb.inline_keyboard.append([InlineKeyboardButton(text="Назад", callback_data="buy_number")])
    
    await message.answer(text, reply_markup=kb)
    await state.clear()


# ==================== АДМИНИСТРАТИВНАЯ ПАНЕЛЬ ====================
@dp.callback_query(F.data == "admin_panel")
async def cb_admin_panel(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.clear()
    try:
        await cb.message.edit_text(
            f'{e("5870982283724328568", "⚙️")} <b>Панель администратора</b>\n\n'
            f'<blockquote>Управление ботом и пользователями</blockquote>',
            reply_markup=admin_main_keyboard()
        )
    except Exception:
        await cb.message.answer(
            f'{e("5870982283724328568", "⚙️")} <b>Панель администратора</b>',
            reply_markup=admin_main_keyboard()
        )
    await cb.answer()


@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    stats = get_stats()
    text = (
        f'{e("5870921681735781843", "📊")} <b>Полная статистика</b>\n\n'
        f'<blockquote>'
        f'{e("5870772616305839506", "👥")} <b>Пользователей всего:</b> {stats["users_total"]}\n'
        f'{e("5870657884844462243", "❌")} <b>Забанено:</b> {stats["users_banned"]}\n'
        f'{e("6037249452824072506", "🔒")} <b>Заморожено:</b> {stats["users_frozen"]}\n'
        f'{e("6032644646587338669", "🎁")} <b>VIP:</b> {stats["users_vip"]}\n'
        f'{e("5870633910337015697", "✅")} <b>Новых за сутки:</b> {stats["new_today"]}\n'
        f'{e("5870633910337015697", "✅")} <b>Новых за неделю:</b> {stats["new_week"]}'
        f'</blockquote>\n\n'
        f'{e("5884479287171485878", "📦")} <b>Активаций:</b> {stats["activations"]}\n'
        f'{e("5870633910337015697", "✅")} <b>Успешных:</b> {stats["success"]}\n\n'
        f'{e("5904462880941545555", "🪙")} <b>Доход за день:</b> {stats["income_day"]:.2f}₽\n'
        f'{e("5904462880941545555", "🪙")} <b>За неделю:</b> {stats["income_week"]:.2f}₽\n'
        f'{e("5904462880941545555", "🪙")} <b>За месяц:</b> {stats["income_month"]:.2f}₽\n'
        f'{e("5904462880941545555", "🪙")} <b>За год:</b> {stats["income_year"]:.2f}₽\n'
        f'{e("5870930636742595124", "📊")} <b>За всё время:</b> {stats["income_all"]:.2f}₽\n'
        f'{e("5890848474563352982", "🪙")} <b>Выведено всего:</b> {stats["total_withdrawn"]:.2f}₽\n'
        f'{e("5879814368572478751", "🏧")} <b>Выдано адм.:</b> {stats["admin_adds"]:.2f}₽\n\n'
        f'{e("5983150113483134607", "⏰")} <b>Ожид. заявок пополн.:</b> {stats["pending_topups"]}\n'
        f'{e("5983150113483134607", "⏰")} <b>Ожид. заявок вывода:</b> {stats["pending_withdraws"]}'
    )
    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_api_balance")
async def cb_admin_api_balance(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    balance = await api_get_balance()
    await cb.answer(f"Баланс API смсфаст: {balance}₽", show_alert=True)


@dp.callback_query(F.data == "admin_api_status")
async def cb_admin_api_status(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    balance = await api_get_balance()
    numbers_status = await api_get_numbers_status("0")
    total_available = sum(numbers_status.values()) if isinstance(numbers_status, dict) else 0
    await cb.message.edit_text(
        f'{e("5940433880585605708", "🔨")} <b>Статус API smsfastapi</b>\n\n'
        f'<blockquote>'
        f'{e("5904462880941545555", "🪙")} <b>Баланс API:</b> {balance}₽\n'
        f'{e("5884479287171485878", "📦")} <b>Доступно номеров (Россия):</b> {total_available}'
        f'</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_toggle_maintenance")
async def cb_toggle_maintenance(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    s = get_settings()
    s["maintenance"] = not s.get("maintenance", False)
    save_settings(s)
    status = "🔴 включено" if s["maintenance"] else "🟢 выключено"
    await cb.answer(f"Техобслуживание {status}", show_alert=True)


@dp.callback_query(F.data == "admin_settings")
async def cb_admin_settings(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    s = get_settings()
    text = (
        f'{e("5870982283724328568", "⚙️")} <b>Текущие настройки</b>\n\n'
        f'<blockquote>'
        f'<b>Название бота:</b> {s.get("bot_name", "Stuffiny SMS")}\n'
        f'<b>Банк:</b> {s["bank_name"]}\n'
        f'<b>Реквизиты:</b> {s["bank_requisites"]}\n'
        f'<b>Получатель:</b> {s["bank_owner"]}\n'
        f'<b>Реф. %:</b> {s["ref_percent"]}%\n'
        f'<b>Мин. пополнение:</b> {s["min_topup"]}₽\n'
        f'<b>Мин. вывод:</b> {s["min_withdraw"]}₽\n'
        f'<b>Наценка:</b> {s["price_markup"]}₽\n'
        f'<b>VIP скидка:</b> {s.get("vip_discount", 0)}%\n'
        f'<b>Макс. номеров:</b> {s.get("max_active_numbers", 10)}\n'
        f'<b>Авто-отмена:</b> {s.get("auto_cancel_minutes", 20)} мин\n'
        f'<b>Страна по умолч.:</b> {s.get("default_country", "0")}\n'
        f'<b>Тех. обслуживание:</b> {"да 🔴" if s.get("maintenance") else "нет 🟢"}'
        f'</blockquote>'
    )
    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_topup_requests")
async def cb_admin_topup_requests(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    pending = [(k, v) for k, v in db.get("topup_requests", {}).items() if v["status"] == "pending"]
    if not pending:
        await cb.answer("Нет новых заявок на пополнение", show_alert=True)
        return
    rows = []
    for req_id, req in pending[-20:]:
        display = f"@{req.get('username')}" if req.get('username') else str(req['uid'])
        rows.append([InlineKeyboardButton(
            text=f"{display} — {req['amount']}₽",
            callback_data=f"admin_view_topup_{req_id}"
        )])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="admin_panel")])
    await cb.message.edit_text(
        f'{e("5879814368572478751", "🏧")} <b>Заявки на пополнение ({len(pending)}):</b>\n\n'
        f'<blockquote>Нажмите на заявку для просмотра</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_withdraw_requests")
async def cb_admin_withdraw_requests(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    pending = [(k, v) for k, v in db.get("withdraw_requests", {}).items() if v["status"] == "pending"]
    if not pending:
        await cb.answer("Нет новых заявок на вывод", show_alert=True)
        return
    rows = []
    for req_id, req in pending[-20:]:
        display = f"@{req.get('username')}" if req.get('username') else str(req['uid'])
        rows.append([InlineKeyboardButton(
            text=f"{display} — {req['amount']}₽",
            callback_data=f"admin_view_withdraw_{req_id}"
        )])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="admin_panel")])
    await cb.message.edit_text(
        f'{e("5890848474563352982", "🪙")} <b>Заявки на вывод ({len(pending)}):</b>\n\n'
        f'<blockquote>Нажмите на заявку для просмотра</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_users")
async def cb_admin_users(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    users = db.get("users", {})
    count = len(users)
    banned = sum(1 for u in users.values() if u.get("banned"))
    frozen = sum(1 for u in users.values() if u.get("frozen"))
    vip = sum(1 for u in users.values() if u.get("vip"))
    await cb.message.edit_text(
        f'{e("5870772616305839506", "👥")} <b>Пользователи</b>\n\n'
        f'<blockquote>'
        f'<b>Всего:</b> {count}\n'
        f'<b>Забанено:</b> {banned}\n'
        f'<b>Заморожено:</b> {frozen}\n'
        f'<b>VIP:</b> {vip}'
        f'</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_all_users")
async def cb_admin_all_users(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    users = sorted(db.get("users", {}).values(), key=lambda u: u.get("joined_at", ""), reverse=True)[:20]
    lines = [f'{e("5870772616305839506", "👥")} <b>Последние 20 пользователей</b>\n']
    for u in users:
        display = get_user_display(u)
        vip_mark = " 🎁" if u.get("vip") else ""
        ban_mark = " ❌" if u.get("banned") else ""
        frz_mark = " 🔒" if u.get("frozen") else ""
        lines.append(f'<code>{u["id"]}</code> {display} — {u["balance"]:.2f}₽{vip_mark}{ban_mark}{frz_mark}')
    await cb.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_top_users")
async def cb_admin_top_users(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    users = sorted(db.get("users", {}).values(), key=lambda u: u.get("total_topup", 0), reverse=True)[:10]
    lines = [f'{e("5870930636742595124", "📊")} <b>Топ 10 пользователей по пополнениям</b>\n']
    medals = ["🥇", "🥈", "🥉"]
    for i, u in enumerate(users, 1):
        display = get_user_display(u)
        medal = medals[i - 1] if i <= 3 else f"{i}."
        lines.append(f'{medal} {display} — {u.get("total_topup", 0):.2f}₽')
    await cb.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_transactions")
async def cb_admin_transactions(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    txs = db.get("transactions", [])[-25:]
    lines = [f'{e("5890937706803894250", "📅")} <b>Последние 25 транзакций</b>\n']
    type_map = {
        "topup": "Пополн.",
        "buy": "Покупка",
        "withdraw": "Вывод",
        "ref_bonus": "Реф.",
        "admin_add": "Адм. выдача",
        "refund": "Возврат",
        "promo": "Промокод",
    }
    for t in reversed(txs):
        sign = "+" if t["type"] in ("topup", "ref_bonus", "admin_add", "refund", "promo") else "-"
        type_label = type_map.get(t["type"], t["type"])
        lines.append(f'<code>{t["uid"]}</code> {sign}{t["amount"]:.2f}₽ {type_label} {t["date"][:10]}')
    await cb.message.edit_text(
        "\n".join(lines) if len(lines) > 1 else f'<blockquote>{e("5870657884844462243", "❌")} Нет транзакций</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_banned_list")
async def cb_admin_banned_list(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    banned = [u for u in db.get("users", {}).values() if u.get("banned")]
    if not banned:
        await cb.answer("Забаненных пользователей нет", show_alert=True)
        return
    lines = [f'{e("5870657884844462243", "❌")} <b>Забаненные ({len(banned)}):</b>\n']
    for u in banned:
        display = get_user_display(u)
        lines.append(f'<code>{u["id"]}</code> {display}')
    await cb.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_frozen_list")
async def cb_admin_frozen_list(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    frozen = [u for u in db.get("users", {}).values() if u.get("frozen")]
    if not frozen:
        await cb.answer("Замороженных пользователей нет", show_alert=True)
        return
    lines = [f'{e("6037249452824072506", "🔒")} <b>Замороженные ({len(frozen)}):</b>\n']
    for u in frozen:
        display = get_user_display(u)
        lines.append(f'<code>{u["id"]}</code> {display}')
    await cb.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_vip_list")
async def cb_admin_vip_list(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    vip_users = [u for u in db.get("users", {}).values() if u.get("vip")]
    if not vip_users:
        await cb.answer("VIP пользователей нет", show_alert=True)
        return
    lines = [f'{e("6032644646587338669", "🎁")} <b>VIP пользователи ({len(vip_users)}):</b>\n']
    for u in vip_users:
        display = get_user_display(u)
        lines.append(f'<code>{u["id"]}</code> {display} — {u["balance"]:.2f}₽')
    await cb.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_all_active")
async def cb_admin_all_active(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    all_active = []
    for uid_str, u in db.get("users", {}).items():
        for num in u.get("active_numbers", []):
            all_active.append((uid_str, u, num))
    if not all_active:
        await cb.answer("Активных номеров нет", show_alert=True)
        return
    lines = [f'{e("5884479287171485878", "📦")} <b>Все активные номера ({len(all_active)}):</b>\n']
    for uid_str, u, num in all_active[:20]:
        display = get_user_display(u)
        lines.append(f'<code>{num["number"]}</code> ({num["service"]}) — {display}')
    await cb.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_export_users")
async def cb_admin_export_users(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    users = db.get("users", {})
    lines = ["ID,Username,FirstName,Balance,TotalTopup,TotalSpent,Activations,Success,VIP,Banned,Frozen,Joined"]
    for u in users.values():
        lines.append(
            f'{u["id"]},'
            f'{u.get("username", "")},'
            f'{u.get("first_name", "")},'
            f'{u["balance"]},'
            f'{u.get("total_topup", 0)},'
            f'{u.get("total_spent", 0)},'
            f'{u["activations_count"]},'
            f'{u.get("success_activations", 0)},'
            f'{u.get("vip", False)},'
            f'{u.get("banned", False)},'
            f'{u.get("frozen", False)},'
            f'{u.get("joined_at", "")[:10]}'
        )
    content = "\n".join(lines).encode("utf-8-sig")
    file = BufferedInputFile(content, filename=f"users_export_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")
    await bot.send_document(cb.from_user.id, file, caption=f'{e("5963103826075456248", "⬆")} <b>Экспорт пользователей</b>\n\nВсего: {len(users)} записей')
    await cb.answer("Файл отправлен")


@dp.callback_query(F.data == "admin_clear_history")
async def cb_admin_clear_history(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    count = len(db.get("transactions", []))
    db["transactions"] = []
    save_db(db)
    await cb.answer(f"История очищена ({count} записей)", show_alert=True)


@dp.callback_query(F.data == "admin_activity_log")
async def cb_admin_activity_log(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    logs = db.get("activity_log", [])[-20:]
    lines = [f'{e("6037397706505195857", "👁")} <b>Лог активности (последние 20)</b>\n']
    for log in reversed(logs):
        lines.append(f'<code>{log["uid"]}</code> {log["action"]} {log["date"][:16]}')
    await cb.message.edit_text(
        "\n".join(lines) if len(lines) > 1 else "Лог пуст",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


# ==================== ИЗБРАННЫЕ СЕРВИСЫ & РЕФ КОДЫ (АДМИН) ====================
@dp.callback_query(F.data == "admin_featured_services")
async def cb_admin_featured_services(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    featured = db.get("featured_services", {})
    text = f'{e("5884479287171485878", "📦")} <b>Избранные сервисы</b>\n\n'
    for code, info in featured.items():
        text += f'<code>{code}</code>: {info.get("name", code)} — {info.get("price", 0)}₽\n'
    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_give_ref_code")
async def cb_admin_give_ref_code(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(AdminStates.find_user)
    await cb.message.edit_text(
        f'{e("5769289093221454192", "🔗")} <b>Выдать кастомный реф код</b>\n\n'
        f'<blockquote>Введите ID или @юзернейм пользователя</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.message(AdminStates.find_user)
async def admin_find_user_for_ref(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    search_text = message.text.strip()
    db = load_db()
    users = db.get("users", {})
    
    found = None
    if search_text.isdigit():
        found = users.get(search_text)
    else:
        username = search_text.lstrip("@")
        for uid, u in users.items():
            if u.get("username", "").lower() == username.lower():
                found = u
                break
    
    if not found:
        await message.answer("❌ Пользователь не найден")
        return
    
    user_id = str(found["id"])
    custom_codes = db.get("custom_ref_codes", {})
    current_code = custom_codes.get(user_id)
    
    text = (
        f'{e("5870994129244131212", "👤")} <b>Пользователь:</b> {get_user_display(found)}\n'
        f'<code>{user_id}</code>\n\n'
        f'{e("5769289093221454192", "🔗")} <b>Текущий реф код:</b> {current_code if current_code else "стандартный"}'
    )
    
    await state.update_data(user_id=user_id)
    await state.set_state(AdminStates.send_to_user)
    await message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Установить новый код", callback_data="admin_set_ref_code")],
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )


@dp.callback_query(F.data == "admin_set_ref_code")
async def cb_set_user_ref_code(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(AdminStates.set_ref_code)
    await cb.message.edit_text(
        f'{e("5769289093221454192", "🔗")} <b>Введите новый реф код</b>\n\n'
        f'<blockquote>Только буквы, цифры, подчёркивание. От 3 до 15 символов.</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.message(AdminStates.set_ref_code)
async def process_set_ref_code(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    
    new_code = message.text.strip()
    data = await state.get_data()
    user_id = data.get("user_id")
    
    # Валидация
    if not new_code or len(new_code) < 3 or len(new_code) > 15:
        await message.answer("❌ Код должен быть от 3 до 15 символов")
        return
    
    if not all(c.isalnum() or c == '_' for c in new_code):
        await message.answer("❌ Используйте только буквы, цифры и подчёркивание")
        return
    
    # Проверяем уникальность
    db = load_db()
    custom_codes = db.get("custom_ref_codes", {})
    
    for uid, code in custom_codes.items():
        if code == new_code and str(uid) != user_id:
            await message.answer("❌ Этот код уже используется!")
            return
    
    # Сохраняем
    custom_codes[user_id] = new_code
    db["custom_ref_codes"] = custom_codes
    save_db(db)
    
    u = get_user(int(user_id))
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start={new_code}"
    
    await message.answer(
        f'{e("5870633910337015697", "✅")} <b>Реф код выдан</b>\n\n'
        f'Пользователь: {get_user_display(u)}\n'
        f'Новый код: <code>{new_code}</code>\n'
        f'Ссылка: <code>{ref_link}</code>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await state.clear()


# ==================== ПРОМОКОДЫ (АДМИН) ====================
@dp.callback_query(F.data == "admin_promo_menu")
async def cb_admin_promo_menu(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = load_db()
    promos = db.get("promo_codes", {})
    lines = [f'{e("6032644646587338669", "🎁")} <b>Промокоды ({len(promos)}):</b>\n']
    for code, promo in promos.items():
        lines.append(f'<code>{code}</code> — {promo["amount"]}₽ × {promo["uses"]} шт.')
    await cb.message.edit_text(
        "\n".join(lines) if len(lines) > 1 else f'<blockquote>{e("6032644646587338669", "🎁")} <b>Промокодов нет</b></blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Создать промокод", callback_data="admin_create_promo", icon_custom_emoji_id="5870633910337015697"),
                InlineKeyboardButton(text="Удалить промокод", callback_data="admin_delete_promo", icon_custom_emoji_id="5870657884844462243"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")],
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_create_promo")
async def cb_admin_create_promo(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(AdminStates.add_promo)
    await cb.message.edit_text(
        f'{e("6032644646587338669", "🎁")} <b>Создание промокода</b>\n\n'
        f'<blockquote>Введите в формате:\n<code>КОД СУММА КОЛИЧЕСТВО</code>\n\nПример: <code>BONUS2025 100 50</code></blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_promo_menu")]
        ])
    )
    await cb.answer()


@dp.message(AdminStates.add_promo)
async def process_create_promo(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        parts = message.text.strip().split()
        code = parts[0].upper()
        amount = float(parts[1])
        uses = int(parts[2])
        db = load_db()
        if "promo_codes" not in db:
            db["promo_codes"] = {}
        db["promo_codes"][code] = {"amount": amount, "uses": uses, "created": datetime.now().isoformat()}
        save_db(db)
        await state.clear()
        await message.answer(
            f'{e("5870633910337015697", "✅")} <b>Промокод создан</b>\n\n'
            f'<blockquote>Код: <code>{code}</code>\nСумма: {amount}₽\nИспользований: {uses}</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
    except Exception:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} <b>Формат: КОД СУММА КОЛИЧЕСТВО</b>\nПример: BONUS2025 100 50</blockquote>'
        )


@dp.callback_query(F.data == "admin_delete_promo")
async def cb_admin_delete_promo(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(AdminStates.delete_promo)
    await cb.message.edit_text(
        f'{e("5870875489362513438", "🗑")} <b>Удаление промокода</b>\n\n'
        f'<blockquote>Введите код для удаления:</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_promo_menu")]
        ])
    )
    await cb.answer()


@dp.message(AdminStates.delete_promo)
async def process_delete_promo(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    code = message.text.strip().upper()
    db = load_db()
    if code in db.get("promo_codes", {}):
        del db["promo_codes"][code]
        save_db(db)
        await state.clear()
        await message.answer(
            f'{e("5870633910337015697", "✅")} Промокод <code>{code}</code> удалён',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
    else:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Промокод не найден</blockquote>'
        )
        await state.clear()


# ==================== ФУНКЦИИ УПРАВЛЕНИЯ ЮЗЕРАМИ (АДМН) ====================
async def admin_ask_uid(cb: CallbackQuery, state: FSMContext, state_name, prompt: str):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(state_name)
    await cb.message.edit_text(
        prompt,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_ban")
async def cb_admin_ban(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.ban_user,
        f'{e("5870657884844462243", "❌")} <b>Бан пользователя</b>\n\n'
        f'<blockquote>Введите ID или @username пользователя для блокировки:</blockquote>'
    )


@dp.message(AdminStates.ban_user)
async def process_ban_user(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        uid = await resolve_user(message.text.strip())
        if uid is None:
            await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Пользователь не найден</blockquote>')
            return
        u = get_user(uid)
        u["banned"] = True
        save_user(uid, u)
        await state.clear()
        display = get_user_display(u)
        await message.answer(
            f'{e("5870657884844462243", "❌")} Пользователь {display} (<code>{uid}</code>) <b>заблокирован</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
        try:
            await bot.send_message(uid, f'<blockquote>{e("5870657884844462243", "❌")} <b>Ваш аккаунт заблокирован</b>\n\nОбратитесь в поддержку.</blockquote>')
        except Exception:
            pass
        log_activity(uid, f"banned_by_{message.from_user.id}")
    except Exception as ex:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Ошибка: {ex}</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="admin_panel")]
            ])
        )


@dp.callback_query(F.data == "admin_unban")
async def cb_admin_unban(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.unban_user,
        f'{e("5870633910337015697", "✅")} <b>Разбан пользователя</b>\n\n'
        f'<blockquote>Введите ID или @username для разблокировки:</blockquote>'
    )


@dp.message(AdminStates.unban_user)
async def process_unban_user(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        uid = await resolve_user(message.text.strip())
        if uid is None:
            await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Пользователь не найден</blockquote>')
            return
        u = get_user(uid)
        u["banned"] = False
        save_user(uid, u)
        await state.clear()
        display = get_user_display(u)
        await message.answer(
            f'{e("5870633910337015697", "✅")} Пользователь {display} (<code>{uid}</code>) <b>разблокирован</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
        try:
            await bot.send_message(uid, f'<blockquote>{e("5870633910337015697", "✅")} <b>Ваш аккаунт разблокирован</b></blockquote>')
        except Exception:
            pass
    except Exception as ex:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Ошибка: {ex}</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="admin_panel")]
            ])
        )


@dp.callback_query(F.data == "admin_freeze")
async def cb_admin_freeze(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.freeze_user,
        f'{e("6037249452824072506", "🔒")} <b>Заморозка аккаунта</b>\n\n'
        f'<blockquote>Введите ID или @username для заморозки:</blockquote>'
    )


@dp.message(AdminStates.freeze_user)
async def process_freeze_user(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        uid = await resolve_user(message.text.strip())
        if uid is None:
            await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Пользователь не найден</blockquote>')
            return
        u = get_user(uid)
        u["frozen"] = True
        save_user(uid, u)
        await state.clear()
        display = get_user_display(u)
        await message.answer(
            f'{e("6037249452824072506", "🔒")} Пользователь {display} (<code>{uid}</code>) <b>заморожен</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
        try:
            await bot.send_message(uid, f'<blockquote>{e("6037249452824072506", "🔒")} <b>Ваш аккаунт временно заморожен</b>\n\nОбратитесь в поддержку.</blockquote>')
        except Exception:
            pass
    except Exception as ex:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Ошибка: {ex}</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="admin_panel")]
            ])
        )


@dp.callback_query(F.data == "admin_unfreeze")
async def cb_admin_unfreeze(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.unfreeze_user,
        f'{e("6037496202990194718", "🔓")} <b>Разморозка аккаунта</b>\n\n'
        f'<blockquote>Введите ID или @username для разморозки:</blockquote>'
    )


@dp.message(AdminStates.unfreeze_user)
async def process_unfreeze_user(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        uid = await resolve_user(message.text.strip())
        if uid is None:
            await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Пользователь не найден</blockquote>')
            return
        u = get_user(uid)
        u["frozen"] = False
        save_user(uid, u)
        await state.clear()
        display = get_user_display(u)
        await message.answer(
            f'{e("6037496202990194718", "🔓")} Пользователь {display} (<code>{uid}</code>) <b>разморожен</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
        try:
            await bot.send_message(uid, f'<blockquote>{e("6037496202990194718", "🔓")} <b>Ваш аккаунт разморожен!</b></blockquote>')
        except Exception:
            pass
    except Exception as ex:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Ошибка: {ex}</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="admin_panel")]
            ])
        )


@dp.callback_query(F.data == "admin_give_balance")
async def cb_admin_give_balance(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.add_balance,
        f'{e("5879814368572478751", "🏧")} <b>Выдача баланса</b>\n\n'
        f'<blockquote>Введите ID/@username и сумму через пробел:\n<code>123456789 100</code>\nили\n<code>@username 100</code></blockquote>'
    )


@dp.message(AdminStates.add_balance)
async def process_add_balance(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        parts = message.text.strip().split()
        uid = await resolve_user(parts[0])
        if uid is None:
            await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Пользователь не найден</blockquote>')
            return
        amount = float(parts[1])
        u = get_user(uid)
        u["balance"] = round(u["balance"] + amount, 2)
        save_user(uid, u)
        add_transaction(uid, amount, "admin_add", f"Выдача от администратора {message.from_user.id}")
        await state.clear()
        display = get_user_display(u)
        await message.answer(
            f'{e("5870633910337015697", "✅")} <b>Добавлено {amount}₽</b> пользователю {display} (<code>{uid}</code>)\n\n'
            f'<blockquote>Новый баланс: {u["balance"]:.2f}₽</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
        try:
            await bot.send_message(
                uid,
                f'{e("5870633910337015697", "✅")} <b>Вам начислено {amount}₽!</b>\n\n'
                f'<blockquote>{e("5904462880941545555", "🪙")} Ваш баланс: {u["balance"]:.2f}₽</blockquote>'
            )
        except Exception:
            pass
    except Exception as ex:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Ошибка: {ex}\nФормат: ID/юзернейм сумма</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="admin_panel")]
            ])
        )


@dp.callback_query(F.data == "admin_take_balance")
async def cb_admin_take_balance(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.sub_balance,
        f'{e("5890848474563352982", "🪙")} <b>Снятие баланса</b>\n\n'
        f'<blockquote>Введите ID/@username и сумму через пробел:</blockquote>'
    )


@dp.message(AdminStates.sub_balance)
async def process_sub_balance(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        parts = message.text.strip().split()
        uid = await resolve_user(parts[0])
        if uid is None:
            await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Пользователь не найден</blockquote>')
            return
        amount = float(parts[1])
        u = get_user(uid)
        old_balance = u["balance"]
        u["balance"] = max(0, round(u["balance"] - amount, 2))
        save_user(uid, u)
        add_transaction(uid, amount, "admin_sub", f"Снятие администратором {message.from_user.id}")
        await state.clear()
        display = get_user_display(u)
        await message.answer(
            f'{e("5870633910337015697", "✅")} <b>Снято {amount}₽</b> у пользователя {display} (<code>{uid}</code>)\n\n'
            f'<blockquote>Было: {old_balance:.2f}₽ → Стало: {u["balance"]:.2f}₽</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
    except Exception as ex:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Ошибка: {ex}\nФормат: ID/юзернейм сумма</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="admin_panel")]
            ])
        )


@dp.callback_query(F.data == "admin_set_balance")
async def cb_admin_set_balance(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.set_balance,
        f'{e("5904462880941545555", "🪙")} <b>Установка баланса</b>\n\n'
        f'<blockquote>Введите ID/@username и новый баланс через пробел:</blockquote>'
    )


@dp.message(AdminStates.set_balance)
async def process_set_balance(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        parts = message.text.strip().split()
        uid = await resolve_user(parts[0])
        if uid is None:
            await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Пользователь не найден</blockquote>')
            return
        new_balance = float(parts[1])
        u = get_user(uid)
        old_balance = u["balance"]
        u["balance"] = round(new_balance, 2)
        save_user(uid, u)
        add_transaction(uid, abs(new_balance - old_balance), "admin_set", f"Установка баланса адм. {message.from_user.id}")
        await state.clear()
        display = get_user_display(u)
        await message.answer(
            f'{e("5870633910337015697", "✅")} Баланс {display} (<code>{uid}</code>) установлен на <b>{new_balance:.2f}₽</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
        try:
            await bot.send_message(uid, f'<blockquote>{e("5904462880941545555", "🪙")} <b>Ваш баланс изменён администратором.</b>\nТекущий баланс: {new_balance:.2f}₽</blockquote>')
        except Exception:
            pass
    except Exception as ex:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Ошибка: {ex}</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="admin_panel")]
            ])
        )


@dp.callback_query(F.data == "admin_find_user")
async def cb_admin_find_user(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.find_user,
        f'{e("5870994129244131212", "👤")} <b>Поиск пользователя</b>\n\n'
        f'<blockquote>Введите ID или @username пользователя:</blockquote>'
    )


@dp.message(AdminStates.find_user)
async def process_find_user(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    query = message.text.strip()
    uid = await resolve_user(query)
    if uid is None:
        await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Пользователь не найден</blockquote>')
        await state.clear()
        return
    found = get_user(uid)
    display = get_user_display(found)
    text = (
        f'{e("5870994129244131212", "👤")} <b>Информация о пользователе</b>\n\n'
        f'<blockquote>'
        f'<b>ID:</b> <code>{found["id"]}</code>\n'
        f'<b>Имя:</b> {found.get("first_name", "—")}\n'
        f'<b>Юзернейм:</b> {display}\n'
        f'<b>VIP:</b> {"да" if found.get("vip") else "нет"}\n'
        f'<b>Забанен:</b> {"да" if found.get("banned") else "нет"}\n'
        f'<b>Заморожен:</b> {"да" if found.get("frozen") else "нет"}\n'
        f'<b>Предупреждений:</b> {found.get("warnings_count", 0)}'
        f'</blockquote>\n\n'
        f'{e("5904462880941545555", "🪙")} <b>Баланс:</b> {found["balance"]:.2f}₽\n'
        f'{e("5879814368572478751", "🏧")} <b>Пополнено:</b> {found.get("total_topup", 0):.2f}₽\n'
        f'{e("5890848474563352982", "🪙")} <b>Потрачено:</b> {found.get("total_spent", 0):.2f}₽\n'
        f'{e("5884479287171485878", "📦")} <b>Активаций:</b> {found["activations_count"]}\n'
        f'{e("5870772616305839506", "👥")} <b>Рефералов:</b> {len(found.get("referrals", []))}\n'
        f'<b>В боте с:</b> {found.get("joined_at", "")[:10]}'
        + (f'\n\n<b>Заметка:</b> {found.get("notes", "")}' if found.get("notes") else '')
    )
    await state.clear()
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
    ]))


@dp.callback_query(F.data == "admin_set_vip")
async def cb_admin_set_vip(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.set_vip,
        f'{e("6032644646587338669", "🎁")} <b>Выдача VIP</b>\n\n'
        f'<blockquote>Введите ID или @username для выдачи VIP статуса:</blockquote>'
    )


@dp.message(AdminStates.set_vip)
async def process_set_vip(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        uid = await resolve_user(message.text.strip())
        if uid is None:
            await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Пользователь не найден</blockquote>')
            return
        u = get_user(uid)
        u["vip"] = True
        save_user(uid, u)
        await state.clear()
        display = get_user_display(u)
        await message.answer(
            f'{e("6032644646587338669", "🎁")} {display} (<code>{uid}</code>) получил <b>VIP статус!</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
        try:
            await bot.send_message(uid, f'<blockquote>{e("6032644646587338669", "🎁")} <b>Вам выдан VIP статус!</b>\nТеперь вы получаете скидку на все номера.</blockquote>')
        except Exception:
            pass
    except Exception as ex:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Ошибка: {ex}</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="admin_panel")]
            ])
        )


@dp.callback_query(F.data == "admin_remove_vip")
async def cb_admin_remove_vip(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.remove_vip,
        f'{e("5870657884844462243", "❌")} <b>Убрать VIP</b>\n\n'
        f'<blockquote>Введите ID или @username для снятия VIP:</blockquote>'
    )


@dp.message(AdminStates.remove_vip)
async def process_remove_vip(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        uid = await resolve_user(message.text.strip())
        if uid is None:
            await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Пользователь не найден</blockquote>')
            return
        u = get_user(uid)
        u["vip"] = False
        save_user(uid, u)
        await state.clear()
        display = get_user_display(u)
        await message.answer(
            f'{e("5870633910337015697", "✅")} VIP статус снят у {display} (<code>{uid}</code>)',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
    except Exception as ex:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Ошибка: {ex}</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="admin_panel")]
            ])
        )


@dp.callback_query(F.data == "admin_add_warning")
async def cb_admin_add_warning(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.add_warning,
        f'{e("6039422865189638057", "📣")} <b>Предупреждение</b>\n\n'
        f'<blockquote>Введите ID/@username и причину через пробел:\n<code>123456789 Спам</code></blockquote>'
    )


@dp.message(AdminStates.add_warning)
async def process_add_warning(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        parts = message.text.strip().split(None, 1)
        uid = await resolve_user(parts[0])
        if uid is None:
            await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Пользователь не найден</blockquote>')
            return
        reason = parts[1] if len(parts) > 1 else "Нарушение правил"
        u = get_user(uid)
        u["warnings_count"] = u.get("warnings_count", 0) + 1
        save_user(uid, u)
        db = load_db()
        if "warnings" not in db:
            db["warnings"] = {}
        if str(uid) not in db["warnings"]:
            db["warnings"][str(uid)] = []
        db["warnings"][str(uid)].append({"reason": reason, "date": datetime.now().isoformat(), "by": message.from_user.id})
        save_db(db)
        await state.clear()
        display = get_user_display(u)
        await message.answer(
            f'{e("6039422865189638057", "📣")} Предупреждение выдано {display} (<code>{uid}</code>)\n'
            f'<blockquote>Причина: {reason}\nВсего предупреждений: {u["warnings_count"]}</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
        try:
            await bot.send_message(uid, f'<blockquote>{e("6039422865189638057", "📣")} <b>Вы получили предупреждение!</b>\nПричина: {reason}\nПредупреждений: {u["warnings_count"]}</blockquote>')
        except Exception:
            pass
    except Exception as ex:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Ошибка: {ex}</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="admin_panel")]
            ])
        )


@dp.callback_query(F.data == "admin_remove_warning")
async def cb_admin_remove_warning(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.remove_warning,
        f'{e("5870633910337015697", "✅")} <b>Снять предупреждение</b>\n\n'
        f'<blockquote>Введите ID или @username:</blockquote>'
    )


@dp.message(AdminStates.remove_warning)
async def process_remove_warning(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        uid = await resolve_user(message.text.strip())
        if uid is None:
            await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Пользователь не найден</blockquote>')
            return
        u = get_user(uid)
        if u.get("warnings_count", 0) > 0:
            u["warnings_count"] -= 1
        save_user(uid, u)
        await state.clear()
        display = get_user_display(u)
        await message.answer(
            f'{e("5870633910337015697", "✅")} Предупреждение снято у {display} (<code>{uid}</code>)\n'
            f'<blockquote>Осталось: {u["warnings_count"]}</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
    except Exception as ex:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Ошибка: {ex}</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="admin_panel")]
            ])
        )


@dp.callback_query(F.data == "admin_add_note")
async def cb_admin_add_note(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.add_note,
        f'{e("5870676941614354370", "🖋")} <b>Заметка о пользователе</b>\n\n'
        f'<blockquote>Введите ID/@username и заметку через пробел:\n<code>123456789 Подозрительный аккаунт</code></blockquote>'
    )


@dp.message(AdminStates.add_note)
async def process_add_note(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        parts = message.text.strip().split(None, 1)
        uid = await resolve_user(parts[0])
        if uid is None:
            await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Пользователь не найден</blockquote>')
            return
        note = parts[1] if len(parts) > 1 else ""
        u = get_user(uid)
        u["notes"] = note
        save_user(uid, u)
        await state.clear()
        display = get_user_display(u)
        await message.answer(
            f'{e("5870633910337015697", "✅")} Заметка добавлена для {display} (<code>{uid}</code>)',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
    except Exception as ex:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Ошибка: {ex}</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="admin_panel")]
            ])
        )


@dp.callback_query(F.data == "admin_message_user")
async def cb_admin_message_user(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.send_to_user,
        f'{e("5870676941614354370", "🖋")} <b>Написать пользователю</b>\n\n'
        f'<blockquote>Введите ID/@username и сообщение через перенос строки:\n<code>ID\nТекст сообщения</code></blockquote>'
    )


@dp.message(AdminStates.send_to_user)
async def process_send_to_user(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    lines = message.text.strip().split("\n", 1)
    if len(lines) < 2:
        await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Формат:\nID или @username\nТекст сообщения</blockquote>')
        return
    try:
        uid = await resolve_user(lines[0].strip())
        if uid is None:
            await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Пользователь не найден</blockquote>')
            return
        text = lines[1].strip()
        await bot.send_message(
            uid,
            f'{e("6039422865189638057", "📣")} <b>Сообщение от администратора:</b>\n\n'
            f'<blockquote>{text}</blockquote>'
        )
        await state.clear()
        await message.answer(
            f'{e("5870633910337015697", "✅")} <b>Сообщение отправлено</b> пользователю <code>{uid}</code>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
    except Exception as ex:
        await message.answer(
            f'<blockquote>{e("5870657884844462243", "❌")} Ошибка: {ex}</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◁ Назад", callback_data="admin_panel")]
            ])
        )


# ==================== НАСТРОЙКИ БОТА (АДМН) ====================
@dp.callback_query(F.data == "admin_edit_welcome")
async def cb_admin_edit_welcome(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(AdminStates.edit_welcome)
    await cb.message.edit_text(
        f'{e("5870676941614354370", "🖋")} <b>Стартовое сообщение</b>\n\n'
        f'<blockquote>Введите новый текст стартового сообщения.\nПоддерживается HTML и премиум эмодзи.</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.message(AdminStates.edit_welcome)
async def process_edit_welcome(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    s = get_settings()
    s["welcome_text"] = message.html_text
    save_settings(s)
    await state.clear()
    await message.answer(
        f'{e("5870633910337015697", "✅")} <b>Стартовое сообщение обновлено</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )


@dp.callback_query(F.data == "admin_bank_settings")
async def cb_admin_bank_settings(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.edit_bank_name,
        f'{e("5879814368572478751", "🏧")} <b>Название банка</b>\n\n'
        f'<blockquote>Введите название банка (например: Сбербанк):</blockquote>'
    )


@dp.message(AdminStates.edit_bank_name)
async def process_edit_bank_name(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    s = get_settings()
    s["bank_name"] = message.text.strip()
    save_settings(s)
    await state.set_state(AdminStates.edit_bank_req)
    await message.answer(
        f'{e("5879814368572478751", "🏧")} <b>Реквизиты</b>\n\n'
        f'<blockquote>Введите реквизиты (номер карты/счёта):</blockquote>'
    )


@dp.message(AdminStates.edit_bank_req)
async def process_edit_bank_req(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    s = get_settings()
    s["bank_requisites"] = message.text.strip()
    save_settings(s)
    await state.set_state(AdminStates.edit_bank_owner)
    await message.answer(
        f'{e("5870994129244131212", "👤")} <b>Получатель</b>\n\n'
        f'<blockquote>Введите имя получателя:</blockquote>'
    )


@dp.message(AdminStates.edit_bank_owner)
async def process_edit_bank_owner(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    s = get_settings()
    s["bank_owner"] = message.text.strip()
    save_settings(s)
    await state.clear()
    await message.answer(
        f'{e("5870633910337015697", "✅")} <b>Банковские реквизиты обновлены</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )


@dp.callback_query(F.data == "admin_ref_percent")
async def cb_admin_ref_percent(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.edit_ref_percent,
        f'{e("5870772616305839506", "👥")} <b>Реферальный процент</b>\n\n'
        f'<blockquote>Введите новый процент (например: 10):</blockquote>'
    )


@dp.message(AdminStates.edit_ref_percent)
async def process_edit_ref_percent(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        val = float(message.text.strip())
        s = get_settings()
        s["ref_percent"] = val
        save_settings(s)
        await state.clear()
        await message.answer(
            f'{e("5870633910337015697", "✅")} Реферальный процент установлен: <b>{val}%</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
    except Exception:
        await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Введите корректное число</blockquote>')


@dp.callback_query(F.data == "admin_min_topup")
async def cb_admin_min_topup(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.edit_min_topup,
        f'{e("5904462880941545555", "🪙")} <b>Мин. сумма пополнения</b>\n\n'
        f'<blockquote>Введите минимальную сумму пополнения в ₽:</blockquote>'
    )


@dp.message(AdminStates.edit_min_topup)
async def process_edit_min_topup(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        val = float(message.text.strip())
        s = get_settings()
        s["min_topup"] = val
        save_settings(s)
        await state.clear()
        await message.answer(
            f'{e("5870633910337015697", "✅")} Мин. пополнение: <b>{val}₽</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
    except Exception:
        await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Введите число</blockquote>')


@dp.callback_query(F.data == "admin_min_withdraw")
async def cb_admin_min_withdraw(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.edit_min_withdraw,
        f'{e("5890848474563352982", "🪙")} <b>Мин. сумма вывода</b>\n\n'
        f'<blockquote>Введите минимальную сумму вывода в ₽:</blockquote>'
    )


@dp.message(AdminStates.edit_min_withdraw)
async def process_edit_min_withdraw(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        val = float(message.text.strip())
        s = get_settings()
        s["min_withdraw"] = val
        save_settings(s)
        await state.clear()
        await message.answer(
            f'{e("5870633910337015697", "✅")} Мин. вывод: <b>{val}₽</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
    except Exception:
        await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Введите число</blockquote>')


@dp.callback_query(F.data == "admin_markup")
async def cb_admin_markup(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.edit_markup,
        f'{e("5870921681735781843", "📊")} <b>Наценка на номера</b>\n\n'
        f'<blockquote>Введите наценку в ₽ (0 = без наценки):</blockquote>'
    )


@dp.message(AdminStates.edit_markup)
async def process_edit_markup(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        val = float(message.text.strip())
        s = get_settings()
        s["price_markup"] = val
        save_settings(s)
        await state.clear()
        await message.answer(
            f'{e("5870633910337015697", "✅")} Наценка: <b>{val}₽</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
    except Exception:
        await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Введите число</blockquote>')


@dp.callback_query(F.data == "admin_support_link")
async def cb_admin_support_link(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.edit_support,
        f'{e("6039422865189638057", "📣")} <b>Ссылка поддержки</b>\n\n'
        f'<blockquote>Введите новую ссылку на поддержку (например: https://t.me/support):</blockquote>'
    )


@dp.message(AdminStates.edit_support)
async def process_edit_support(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    s = get_settings()
    s["support_link"] = message.text.strip()
    save_settings(s)
    await state.clear()
    await message.answer(
        f'{e("5870633910337015697", "✅")} <b>Ссылка поддержки обновлена</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )


@dp.callback_query(F.data == "admin_cryptobot_token")
async def cb_admin_cryptobot_token(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.edit_cryptobot,
        f'{e("5084754787518383579", "🤖")} <b>CryptoBot токен</b>\n\n'
        f'<blockquote>Введите токен CryptoBot (@CryptoBot):</blockquote>'
    )


@dp.message(AdminStates.edit_cryptobot)
async def process_edit_cryptobot(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    s = get_settings()
    s["cryptobot_token"] = message.text.strip()
    save_settings(s)
    await state.clear()
    await message.answer(
        f'{e("5870633910337015697", "✅")} <b>CryptoBot токен обновлён</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )


@dp.callback_query(F.data == "admin_edit_bot_name")
async def cb_admin_edit_bot_name(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.edit_bot_name,
        f'{e("5870676941614354370", "🖋")} <b>Название бота</b>\n\n'
        f'<blockquote>Введите новое название бота:</blockquote>'
    )


@dp.message(AdminStates.edit_bot_name)
async def process_edit_bot_name(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    s = get_settings()
    s["bot_name"] = message.text.strip()
    save_settings(s)
    await state.clear()
    await message.answer(
        f'{e("5870633910337015697", "✅")} Название бота: <b>{s["bot_name"]}</b>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ])
    )


@dp.callback_query(F.data == "admin_vip_discount")
async def cb_admin_vip_discount(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.edit_vip_discount,
        f'{e("6032644646587338669", "🎁")} <b>VIP скидка</b>\n\n'
        f'<blockquote>Введите скидку для VIP пользователей в % (например: 5):</blockquote>'
    )


@dp.message(AdminStates.edit_vip_discount)
async def process_edit_vip_discount(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        val = float(message.text.strip())
        s = get_settings()
        s["vip_discount"] = val
        save_settings(s)
        await state.clear()
        await message.answer(
            f'{e("5870633910337015697", "✅")} VIP скидка: <b>{val}%</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
    except Exception:
        await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Введите число</blockquote>')


@dp.callback_query(F.data == "admin_max_numbers")
async def cb_admin_max_numbers(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.edit_max_numbers,
        f'{e("5884479287171485878", "📦")} <b>Макс. активных номеров</b>\n\n'
        f'<blockquote>Введите максимальное количество активных номеров на одного пользователя:</blockquote>'
    )


@dp.message(AdminStates.edit_max_numbers)
async def process_edit_max_numbers(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        val = int(message.text.strip())
        s = get_settings()
        s["max_active_numbers"] = val
        save_settings(s)
        await state.clear()
        await message.answer(
            f'{e("5870633910337015697", "✅")} Макс. активных номеров: <b>{val}</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
    except Exception:
        await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Введите целое число</blockquote>')


@dp.callback_query(F.data == "admin_auto_cancel")
async def cb_admin_auto_cancel(cb: CallbackQuery, state: FSMContext):
    await admin_ask_uid(
        cb, state, AdminStates.edit_auto_cancel,
        f'{e("5983150113483134607", "⏰")} <b>Авто-отмена</b>\n\n'
        f'<blockquote>Введите время (в минутах) до авто-отмены номера без SMS (например: 20):</blockquote>'
    )


@dp.message(AdminStates.edit_auto_cancel)
async def process_edit_auto_cancel(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        val = int(message.text.strip())
        s = get_settings()
        s["auto_cancel_minutes"] = val
        save_settings(s)
        await state.clear()
        await message.answer(
            f'{e("5870633910337015697", "✅")} Авто-отмена через: <b>{val} мин.</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
    except Exception:
        await message.answer(f'<blockquote>{e("5870657884844462243", "❌")} Введите целое число</blockquote>')


# ==================== РАССЫЛКА ====================
@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return await cb.answer()
    await state.set_state(BroadcastForm.waiting_for_text)
    msg = await cb.message.edit_text(
        f'<blockquote>{e("6039422865189638057", "📣")} <b>Шаг 1: Отправьте текст рассылки</b>\n\nПоддерживаются HTML и премиум эмодзи.</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]
        ]),
    )
    await state.update_data(step1_msg_id=msg.message_id)
    await cb.answer()


@dp.message(BroadcastForm.waiting_for_text)
async def bc_receive_text(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    data = await state.get_data()
    try:
        await bot.delete_message(msg.chat.id, data.get("step1_msg_id"))
    except Exception:
        pass
    try:
        await msg.delete()
    except Exception:
        pass
    await state.update_data(bc_text=msg.text or msg.caption, bc_entities=msg.entities or msg.caption_entities)
    await state.set_state(BroadcastForm.waiting_for_media)
    step_msg = await bot.send_message(
        msg.chat.id,
        f'<blockquote>{e("6039422865189638057", "📣")} <b>Шаг 2: Прикрепите медиа или пропустите</b></blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Пропустить", callback_data="bc_skip_media", icon_custom_emoji_id="5870633910337015697")],
            [InlineKeyboardButton(text="Отмена", callback_data="admin_panel")],
        ]),
    )
    await state.update_data(step2_msg_id=step_msg.message_id)


@dp.message(BroadcastForm.waiting_for_media, F.photo | F.video | F.animation)
async def bc_receive_media(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    data = await state.get_data()
    try:
        await bot.delete_message(msg.chat.id, data.get("step2_msg_id"))
    except Exception:
        pass
    try:
        await msg.delete()
    except Exception:
        pass
    if msg.photo:
        await state.update_data(bc_photo=msg.photo[-1].file_id, bc_video=None, bc_animation=None)
    elif msg.video:
        await state.update_data(bc_photo=None, bc_video=msg.video.file_id, bc_animation=None)
    elif msg.animation:
        await state.update_data(bc_photo=None, bc_video=None, bc_animation=msg.animation.file_id)
    await _bc_ask_button(msg.chat.id, state)


@dp.callback_query(F.data == "bc_skip_media", BroadcastForm.waiting_for_media)
async def bc_skip_media(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return await cb.answer()
    data = await state.get_data()
    try:
        await bot.delete_message(cb.message.chat.id, data.get("step2_msg_id"))
    except Exception:
        pass
    await state.update_data(bc_photo=None, bc_video=None, bc_animation=None)
    await _bc_ask_button(cb.message.chat.id, state)
    await cb.answer()


async def _bc_ask_button(chat_id: int, state: FSMContext):
    await state.set_state(BroadcastForm.waiting_for_btn_text)
    step_msg = await bot.send_message(
        chat_id,
        f'<blockquote>{e("6039422865189638057", "📣")} <b>Шаг 3: Добавить кнопку со ссылкой?</b></blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Добавить кнопку", callback_data="bc_add_button", icon_custom_emoji_id="5870633910337015697")],
            [InlineKeyboardButton(text="Без кнопки", callback_data="bc_skip_button", icon_custom_emoji_id="5870657884844462243")],
            [InlineKeyboardButton(text="Отмена", callback_data="admin_panel")],
        ]),
    )
    await state.update_data(step3_msg_id=step_msg.message_id)


@dp.callback_query(F.data == "bc_add_button", BroadcastForm.waiting_for_btn_text)
async def bc_add_button(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return await cb.answer()
    data = await state.get_data()
    try:
        await bot.delete_message(cb.message.chat.id, data.get("step3_msg_id"))
    except Exception:
        pass
    step_msg = await bot.send_message(
        cb.message.chat.id,
        f'<blockquote>{e("6039422865189638057", "📣")} <b>Шаг 4: Введите текст кнопки:</b></blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]
        ]),
    )
    await state.update_data(step4_msg_id=step_msg.message_id)
    await cb.answer()


@dp.message(BroadcastForm.waiting_for_btn_text)
async def bc_receive_btn_text(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    data = await state.get_data()
    btn_text = msg.text or ""
    btn_emoji_id = None
    if msg.entities:
        for entity in msg.entities:
            if entity.type == "custom_emoji":
                btn_emoji_id = entity.custom_emoji_id
                btn_text = (btn_text[:entity.offset] + btn_text[entity.offset + entity.length:]).strip()
                break
    if not btn_text and not btn_emoji_id:
        await msg.answer(f'<blockquote>{e("5870657884844462243", "❌")} <b>Текст кнопки не может быть пустым</b></blockquote>')
        return
    await state.update_data(bc_btn_text=btn_text, bc_btn_emoji_id=btn_emoji_id)
    try:
        await bot.delete_message(msg.chat.id, data.get("step4_msg_id"))
    except Exception:
        pass
    try:
        await msg.delete()
    except Exception:
        pass
    await state.set_state(BroadcastForm.waiting_for_btn_url)
    step_msg = await bot.send_message(
        msg.chat.id,
        f'<blockquote>{e("6039422865189638057", "📣")} <b>Шаг 5: Введите URL для кнопки:</b></blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]
        ]),
    )
    await state.update_data(step5_msg_id=step_msg.message_id)


@dp.message(BroadcastForm.waiting_for_btn_url)
async def bc_receive_btn_url(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    data = await state.get_data()
    try:
        await bot.delete_message(msg.chat.id, data.get("step5_msg_id"))
    except Exception:
        pass
    try:
        await msg.delete()
    except Exception:
        pass
    await state.update_data(bc_btn_url=msg.text)
    await _bc_show_preview(msg.chat.id, msg.from_user.id, state)


@dp.callback_query(F.data == "bc_skip_button", BroadcastForm.waiting_for_btn_text)
async def bc_skip_button(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return await cb.answer()
    data = await state.get_data()
    try:
        await bot.delete_message(cb.message.chat.id, data.get("step3_msg_id"))
    except Exception:
        pass
    await state.update_data(bc_btn_text=None, bc_btn_url=None)
    await _bc_show_preview(cb.message.chat.id, cb.from_user.id, state)
    await cb.answer()


async def _bc_show_preview(chat_id: int, user_id: int, state: FSMContext):
    data = await state.get_data()
    bc_text = data.get("bc_text")
    bc_entities = data.get("bc_entities")
    bc_photo = data.get("bc_photo")
    bc_video = data.get("bc_video")
    bc_animation = data.get("bc_animation")
    btn_text = data.get("bc_btn_text")
    btn_url = data.get("bc_btn_url")
    btn_emoji_id = data.get("bc_btn_emoji_id")
    reply_markup = _bc_build_reply_markup(btn_text, btn_url, btn_emoji_id)
    await bot.send_message(chat_id, f'<blockquote>{e("6037397706505195857", "👁")} <b>Предпросмотр рассылки:</b></blockquote>')
    if bc_photo:
        preview = await bot.send_photo(chat_id, bc_photo, caption=bc_text, caption_entities=bc_entities, reply_markup=reply_markup, parse_mode=None)
    elif bc_video:
        preview = await bot.send_video(chat_id, bc_video, caption=bc_text, caption_entities=bc_entities, reply_markup=reply_markup, parse_mode=None)
    elif bc_animation:
        preview = await bot.send_animation(chat_id, bc_animation, caption=bc_text, caption_entities=bc_entities, reply_markup=reply_markup, parse_mode=None)
    else:
        preview = await bot.send_message(chat_id, bc_text, entities=bc_entities, reply_markup=reply_markup, parse_mode=None)
    confirm_msg = await bot.send_message(
        chat_id,
        f'<blockquote>{e("6039422865189638057", "📣")} <b>Подтвердить рассылку?</b></blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Отправить", callback_data="bc_confirm", icon_custom_emoji_id="5870633910337015697"),
            InlineKeyboardButton(text="Отмена", callback_data="bc_cancel_preview", icon_custom_emoji_id="5870657884844462243"),
        ]]),
    )
    await state.update_data(preview_msg_id=preview.message_id, confirm_msg_id=confirm_msg.message_id)


@dp.callback_query(F.data == "bc_confirm")
async def bc_confirm(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return await cb.answer()
    data = await state.get_data()
    for mid in [data.get("confirm_msg_id"), data.get("preview_msg_id")]:
        if mid:
            try:
                await bot.delete_message(cb.message.chat.id, mid)
            except Exception:
                pass
    bc_text = data.get("bc_text")
    bc_entities = data.get("bc_entities")
    bc_photo = data.get("bc_photo")
    bc_video = data.get("bc_video")
    bc_animation = data.get("bc_animation")
    btn_text = data.get("bc_btn_text")
    btn_url = data.get("bc_btn_url")
    btn_emoji_id = data.get("bc_btn_emoji_id")
    await state.clear()
    reply_markup = _bc_build_reply_markup(btn_text, btn_url, btn_emoji_id)
    users = get_all_broadcast_users()
    success = failed = 0
    status_msg = await bot.send_message(
        cb.message.chat.id,
        f'<blockquote>{e("6039422865189638057", "📣")} <b>Рассылка начата…</b>\nПользователей: {len(users)}</blockquote>',
    )
    for uid in users:
        try:
            if bc_photo:
                await bot.send_photo(uid, bc_photo, caption=bc_text, caption_entities=bc_entities, reply_markup=reply_markup, parse_mode=None)
            elif bc_video:
                await bot.send_video(uid, bc_video, caption=bc_text, caption_entities=bc_entities, reply_markup=reply_markup, parse_mode=None)
            elif bc_animation:
                await bot.send_animation(uid, bc_animation, caption=bc_text, caption_entities=bc_entities, reply_markup=reply_markup, parse_mode=None)
            else:
                await bot.send_message(uid, bc_text, entities=bc_entities, reply_markup=reply_markup, parse_mode=None)
            success += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await status_msg.edit_text(
        f'{e("5870633910337015697", "✅")} <b>Рассылка завершена</b>\n\n'
        f'<blockquote>'
        f'{e("5870633910337015697", "✅")} <b>Успешно:</b> {success}\n'
        f'{e("5870657884844462243", "❌")} <b>Ошибок:</b> {failed}'
        f'</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
        ]),
    )
    await cb.answer()


@dp.callback_query(F.data == "bc_cancel_preview")
async def bc_cancel_preview(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return await cb.answer()
    data = await state.get_data()
    for mid in [data.get("confirm_msg_id"), data.get("preview_msg_id")]:
        if mid:
            try:
                await bot.delete_message(cb.message.chat.id, mid)
            except Exception:
                pass
    await state.clear()
    await bot.send_message(
        cb.message.chat.id,
        f'{e("5870982283724328568", "⚙️")} <b>Панель администратора</b>',
        reply_markup=admin_main_keyboard()
    )
    await cb.answer()


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
async def resolve_user(query: str):
    """Найти пользователя по ID или @username"""
    query = query.strip()
    db = load_db()

    if query.startswith("@"):
        username = query[1:].lower()
        for uid_str, u in db["users"].items():
            if u.get("username") and u["username"].lower() == username:
                return int(uid_str)
        return None

    try:
        uid = int(query)
        if str(uid) in db["users"]:
            return uid
        return None
    except ValueError:
        return None


# ==================== NOOP И ПРОЧИЕ ====================
@dp.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()


@dp.callback_query(F.data == "ref_send")
async def cb_ref_send(cb: CallbackQuery):
    uid = cb.from_user.id
    u = get_user(uid)
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start={u['ref_code']}"
    await cb.answer(f"Ваша ссылка:\n{ref_link}", show_alert=True)



# ==================== УПРАВЛЕНИЕ СЕРВИСАМИ ====================
@dp.callback_query(F.data == "admin_manage_services")
async def admin_manage_services(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    
    db = load_db()
    
    # Загружаем ВСЕ сервисы из разных источников
    all_services = {}
    
    # 1. Из API
    api_services = await api_get_services("187")
    if api_services:
        if isinstance(api_services, list):
            for svc in api_services:
                if isinstance(svc, dict):
                    code = svc.get("code", svc.get("id"))
                    if code:
                        all_services[code] = {
                            "name": capitalize_service_name(svc.get("name", code)),
                            "price": svc.get("price", 0),
                            "count": svc.get("count", 0),
                            "source": "API"
                        }
        elif isinstance(api_services, dict):
            for code, data in api_services.items():
                all_services[code] = {
                    "name": capitalize_service_name(data.get("name", code)),
                    "price": data.get("price", 0),
                    "count": data.get("count", 0),
                    "source": "API"
                }
    
    # 2. Из кэша (db.json)
    cached = db.get("cached_services", {})
    for code, data in cached.items():
        if code not in all_services:
            all_services[code] = {
                "name": capitalize_service_name(data.get("name", code)),
                "price": data.get("price", 0),
                "count": data.get("count", 0),
                "source": "Cache"
            }
    
    # 3. Из hardcoded fallback
    hardcoded_services = await fetch_services_hardcoded()
    if hardcoded_services and isinstance(hardcoded_services, dict):
        for code, data in hardcoded_services.items():
            if code not in all_services:
                all_services[code] = {
                    "name": capitalize_service_name(data.get("name", code)),
                    "price": data.get("price", 0),
                    "count": data.get("count", 0),
                    "source": "Fallback"
                }
    
    if not all_services:
        await cb.message.edit_text(
            f'{e("5870657884844462243", "❌")} <b>Нет сервисов</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel", icon_custom_emoji_id="5893057118545646106")]
            ])
        )
        return
    
    # Сохраняем ВСЕ сервисы в кэш
    db["cached_services"] = {code: {"name": data["name"], "price": data["price"], "count": data["count"]} for code, data in all_services.items()}
    save_db(db)
    
    # Показываем список сервисов для редактирования
    services_list = sorted(all_services.items(), key=lambda x: (get_service_priority(x[0]), capitalize_service_name(x[1].get("name", x[0]))))
    total = len(services_list)
    api_count = sum(1 for s in all_services.values() if s.get("source") == "API")
    cache_count = sum(1 for s in all_services.values() if s.get("source") == "Cache")
    fallback_count = sum(1 for s in all_services.values() if s.get("source") == "Fallback")
    
    text = (
        f'{e("5884479287171485878", "📦")} <b>Управление сервисами</b>\n\n'
        f'<blockquote>'
        f'📊 <b>Всего:</b> {total}\n'
        f'🌐 <b>API:</b> {api_count}\n'
        f'💾 <b>Кэш:</b> {cache_count}\n'
        f'⚙️ <b>Fallback:</b> {fallback_count}\n\n'
        f'Выберите сервис для редактирования:'
        f'</blockquote>'
    )
    
    rows = []
    for code, data in services_list[:15]:  # First 15
        name = capitalize_service_name(data.get("name", code))
        price = data.get("price", 0)
        source = data.get("source", "?")
        btn = InlineKeyboardButton(
            text=f"{name} ({price}₽) [{source[0]}]",
            callback_data=f"admin_edit_service_{code}"
        )
        rows.append([btn])
    
    if len(services_list) > 15:
        rows.append([InlineKeyboardButton(text=f"... ещё {len(services_list) - 15}", callback_data="admin_manage_services")])
    
    rows.append([InlineKeyboardButton(text="Назад", callback_data="admin_panel", icon_custom_emoji_id="5893057118545646106")])
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()


@dp.callback_query(F.data.startswith("admin_edit_service_"))
async def admin_edit_service(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    
    service_code = cb.data.replace("admin_edit_service_", "")
    db = load_db()
    cached = db.get("cached_services", {})
    
    if service_code not in cached:
        await cb.answer("Сервис не найден", show_alert=True)
        return
    
    svc = cached[service_code]
    name = svc.get("name", service_code)
    price = svc.get("price", 0)
    count = svc.get("count", 0)
    
    text = (
        f'{e("5884479287171485878", "📦")} <b>Редактирование сервиса</b>\n\n'
        f'<blockquote>'
        f'<b>Код:</b> <code>{service_code}</code>\n'
        f'<b>Название:</b> {name}\n'
        f'<b>Цена:</b> {price}₽\n'
        f'<b>Количество:</b> {count} шт'
        f'</blockquote>\n\n'
        f'<b>Выберите, что редактировать:</b>'
    )
    
    rows = [
        [
            InlineKeyboardButton(text="📝 Название", callback_data=f"admin_edit_svc_name_{service_code}"),
            InlineKeyboardButton(text="Цена", callback_data=f"admin_edit_svc_price_{service_code}"),
        ],
        [
            InlineKeyboardButton(text="📦 Количество", callback_data=f"admin_edit_svc_count_{service_code}"),
        ],
        [InlineKeyboardButton(text="Назад", callback_data="admin_manage_services", icon_custom_emoji_id="5893057118545646106")],
    ]
    
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()


@dp.callback_query(F.data.startswith("admin_edit_svc_name_"))
async def admin_edit_svc_name(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    
    service_code = cb.data.replace("admin_edit_svc_name_", "")
    await state.set_state(AdminServiceStates.editing_svc_name)
    await state.update_data(service_code=service_code)
    
    await cb.message.edit_text(
        f'<blockquote>Введите новое название сервиса для <code>{service_code}</code>:</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_manage_services", icon_custom_emoji_id="5893057118545646106")]
        ])
    )
    await cb.answer()


@dp.message(AdminServiceStates.editing_svc_name)
async def process_svc_name(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    
    data = await state.get_data()
    service_code = data.get("service_code")
    new_name = message.text.strip()
    
    db = load_db()
    if service_code in db.get("cached_services", {}):
        db["cached_services"][service_code]["name"] = new_name
        save_db(db)
        
        await message.answer(
            f'{e("5870633910337015697", "✅")} <b>Название обновлено!</b>\n\n'
            f'Сервис: <code>{service_code}</code>\n'
            f'Новое название: <b>{new_name}</b>'
        )
    else:
        await message.answer(f'{e("5870657884844462243", "❌")} Сервис не найден')
    
    await state.clear()


@dp.callback_query(F.data.startswith("admin_edit_svc_price_"))
async def admin_edit_svc_price(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    
    service_code = cb.data.replace("admin_edit_svc_price_", "")
    await state.set_state(AdminServiceStates.editing_svc_price)
    await state.update_data(service_code=service_code)
    
    await cb.message.edit_text(
        f'<blockquote>Введите новую цену для сервиса <code>{service_code}</code>:\n(например: 15.5)</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_manage_services", icon_custom_emoji_id="5893057118545646106")]
        ])
    )
    await cb.answer()


@dp.message(AdminServiceStates.editing_svc_price)
async def process_svc_price(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    
    try:
        new_price = float(message.text.strip())
        if new_price < 0:
            raise ValueError("Цена не может быть отрицательной")
    except ValueError as ex:
        await message.answer(f'{e("5870657884844462243", "❌")} Ошибка: введите корректное число\n{str(ex)}')
        return
    
    data = await state.get_data()
    service_code = data.get("service_code")
    
    db = load_db()
    if service_code in db.get("cached_services", {}):
        db["cached_services"][service_code]["price"] = new_price
        save_db(db)
        
        await message.answer(
            f'{e("5870633910337015697", "✅")} <b>Цена обновлена!</b>\n\n'
            f'Сервис: <code>{service_code}</code>\n'
            f'Новая цена: <b>{new_price}₽</b>'
        )
    else:
        await message.answer(f'{e("5870657884844462243", "❌")} Сервис не найден')
    
    await state.clear()


@dp.callback_query(F.data.startswith("admin_edit_svc_count_"))
async def admin_edit_svc_count(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    
    service_code = cb.data.replace("admin_edit_svc_count_", "")
    await state.set_state(AdminServiceStates.editing_svc_count)
    await state.update_data(service_code=service_code)
    
    await cb.message.edit_text(
        f'<blockquote>Введите новое количество номеров для <code>{service_code}</code>:\n(целое число)</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_manage_services", icon_custom_emoji_id="5893057118545646106")]
        ])
    )
    await cb.answer()


@dp.message(AdminServiceStates.editing_svc_count)
async def process_svc_count(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    
    try:
        new_count = int(message.text.strip())
        if new_count < 0:
            raise ValueError("Количество не может быть отрицательным")
    except ValueError as ex:
        await message.answer(f'{e("5870657884844462243", "❌")} Ошибка: введите целое число\n{str(ex)}')
        return
    
    data = await state.get_data()
    service_code = data.get("service_code")
    
    db = load_db()
    if service_code in db.get("cached_services", {}):
        db["cached_services"][service_code]["count"] = new_count
        save_db(db)
        
        await message.answer(
            f'{e("5870633910337015697", "✅")} <b>Количество обновлено!</b>\n\n'
            f'Сервис: <code>{service_code}</code>\n'
            f'Новое количество: <b>{new_count} шт</b>'
        )
    else:
        await message.answer(f'{e("5870657884844462243", "❌")} Сервис не найден')
    
    await state.clear()


# ==================== ПОИСК ЗАКАЗОВ ДЛЯ АДМИНОВ ====================
@dp.callback_query(F.data == "admin_find_order")
async def cb_admin_find_order(cb: CallbackQuery, state: FSMContext):
    """Поиск заказа по ID"""
    if not is_admin(cb.from_user.id):
        return
    
    await state.set_state(AdminStates.find_user)
    await cb.message.edit_text(
        f'{e("5884479287171485878", "📦")} <b>Поиск заказа</b>\n\n'
        f'<blockquote>Введите ID заказа (например: ORD-A1B2C3D4)</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]
        ])
    )
    await cb.answer()


@dp.message(AdminStates.find_user)
async def process_find_order(message: Message, state: FSMContext):
    """Обработка поиска заказа"""
    if not is_admin(message.from_user.id):
        return
    
    order_id = message.text.strip().upper()
    db = load_db()
    orders = db.get("orders", {})
    
    if order_id in orders:
        order = orders[order_id]
        user_id = order.get("user_id")
        user = get_user(user_id)
        
        text = (
            f'{e("5884479287171485878", "📦")} <b>Заказ найден!</b>\n\n'
            f'<blockquote>'
            f'<b>ID заказа:</b> <code>{order.get("order_id", "N/A")}</code>\n'
            f'<b>ID активации:</b> <code>{order.get("id", "N/A")}</code>\n'
            f'<b>User ID:</b> <code>{user_id}</code>\n'
            f'<b>Юзернейм:</b> @{user.get("username", "N/A")}\n'
            f'<b>Номер:</b> <code>{order.get("number", "N/A")}</code>\n'
            f'<b>Сервис:</b> {order.get("service", "N/A")}\n'
            f'<b>Цена:</b> {order.get("price", 0):.2f}₽\n'
            f'<b>Статус:</b> {order.get("status", "unknown")}\n'
            f'<b>Дата:</b> {order.get("date", "N/A")[:19]}'
            f'</blockquote>'
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"Профиль юзера", callback_data=f"user_profile_{user_id}")],
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")],
        ])
    else:
        text = (
            f'{e("5870657884844462243", "❌")} <b>Заказ не найден</b>\n\n'
            f'<blockquote>Заказ с ID <code>{order_id}</code> не существует.</blockquote>'
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Попробовать снова", callback_data="admin_find_order")],
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")],
        ])
    
    await message.answer(text, reply_markup=kb)
    await state.clear()


@dp.callback_query(F.data == "admin_support_tickets")
async def cb_admin_support_tickets(cb: CallbackQuery):
    """Просмотр всех жалоб"""
    if not is_admin(cb.from_user.id):
        return
    
    db = load_db()
    tickets = db.get("support_tickets", {})
    
    if not tickets:
        text = (
            f'{e("6039422865189638057", "📞")} <b>Жалобы и обращения</b>\n\n'
            f'<blockquote>Нет открытых жалоб</blockquote>'
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_panel")],
        ])
    else:
        text_lines = [f'{e("6039422865189638057", "📞")} <b>Жалобы и обращения</b>\n']
        text_lines.append(f'<blockquote>Всего: {len(tickets)}</blockquote>\n')
        
        # Показываем последние 10 жалоб
        for ticket_id, ticket in list(tickets.items())[-10:]:
            status_emoji = "🟢" if ticket.get("status") == "open" else "🟡"
            text_lines.append(
                f'{status_emoji} <b>{ticket_id}</b> - {ticket.get("reason_text", "Другое")}\n'
                f'  Юзер: @{ticket.get("username", "Unknown")}\n'
                f'  Дата: {ticket.get("date", "N/A")[:10]}\n'
            )
        
        text = "".join(text_lines)
        
        rows = []
        for ticket_id in list(tickets.keys())[-10:]:
            rows.append([InlineKeyboardButton(
                text=f"🔍 {ticket_id}",
                callback_data=f"admin_view_ticket_{ticket_id}"
            )])
        rows.append([InlineKeyboardButton(text="Назад", callback_data="admin_panel")])
        kb = InlineKeyboardMarkup(inline_keyboard=rows)
    
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@dp.callback_query(F.data.startswith("admin_view_ticket_"))
async def cb_admin_view_ticket(cb: CallbackQuery):
    """Просмотр деталей жалобы"""
    if not is_admin(cb.from_user.id):
        return
    
    ticket_id = cb.data.replace("admin_view_ticket_", "")
    db = load_db()
    tickets = db.get("support_tickets", {})
    
    if ticket_id not in tickets:
        await cb.answer("Жалоба не найдена", show_alert=True)
        return
    
    ticket = tickets[ticket_id]
    user_id = ticket.get("user_id")
    user = get_user(user_id)
    
    text = (
        f'{e("6039422865189638057", "📞")} <b>Жалоба {ticket_id}</b>\n\n'
        f'<blockquote>'
        f'<b>Статус:</b> {ticket.get("status", "unknown").upper()}\n'
        f'<b>Категория:</b> {ticket.get("reason_text", "Другое")}\n'
        f'<b>User ID:</b> <code>{user_id}</code>\n'
        f'<b>Юзернейм:</b> @{ticket.get("username", "Unknown")}\n'
        f'<b>Дата обращения:</b> {ticket.get("date", "N/A")[:19]}\n\n'
        f'<b>Описание:</b>\n{ticket.get("description", "N/A")}'
        f'</blockquote>'
    )
    
    kb_rows = [
        [InlineKeyboardButton(text="📱 Профиль юзера", callback_data=f"user_profile_{user_id}")],
    ]
    
    if ticket.get("screenshot"):
        kb_rows.append([InlineKeyboardButton(text="🖼️ Скрин", callback_data=f"view_ticket_screenshot_{ticket_id}")])
    
    if ticket.get("status") == "open":
        kb_rows.append([InlineKeyboardButton(text="✅ Закрыть", callback_data=f"close_ticket_{ticket_id}")])
    else:
        kb_rows.append([InlineKeyboardButton(text="🔄 Переоткрыть", callback_data=f"reopen_ticket_{ticket_id}")])
    
    kb_rows.append([InlineKeyboardButton(text="◁ Назад", callback_data="admin_support_tickets")])
    
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@dp.callback_query(F.data.startswith("close_ticket_"))
async def cb_close_ticket(cb: CallbackQuery):
    """Закрыть жалобу"""
    if not is_admin(cb.from_user.id):
        return
    
    ticket_id = cb.data.replace("close_ticket_", "")
    db = load_db()
    
    if ticket_id in db.get("support_tickets", {}):
        db["support_tickets"][ticket_id]["status"] = "closed"
        save_db(db)
        await cb.answer("✅ Жалоба закрыта", show_alert=True)
        await cb_admin_view_ticket(cb)
    else:
        await cb.answer("Жалоба не найдена", show_alert=True)


@dp.callback_query(F.data.startswith("reopen_ticket_"))
async def cb_reopen_ticket(cb: CallbackQuery):
    """Переоткрыть жалобу"""
    if not is_admin(cb.from_user.id):
        return
    
    ticket_id = cb.data.replace("reopen_ticket_", "")
    db = load_db()
    
    if ticket_id in db.get("support_tickets", {}):
        db["support_tickets"][ticket_id]["status"] = "open"
        save_db(db)
        await cb.answer("🔄 Жалоба переоткрыта", show_alert=True)
        await cb_admin_view_ticket(cb)
    else:
        await cb.answer("Жалоба не найдена", show_alert=True)


@dp.callback_query(F.data.startswith("view_ticket_screenshot_"))
async def cb_view_ticket_screenshot(cb: CallbackQuery):
    """Просмотр скрина жалобы"""
    if not is_admin(cb.from_user.id):
        return
    
    ticket_id = cb.data.replace("view_ticket_screenshot_", "")
    db = load_db()
    tickets = db.get("support_tickets", {})
    
    if ticket_id not in tickets or not tickets[ticket_id].get("screenshot"):
        await cb.answer("Скрин не найден", show_alert=True)
        return
    
    screenshot_id = tickets[ticket_id]["screenshot"]
    await bot.send_photo(
        chat_id=cb.from_user.id,
        photo=screenshot_id,
        caption=f"Скрин из жалобы {ticket_id}"
    )
    await cb.answer()


# ==================== УПРАВЛЕНИЕ ПОДПИСКАМИ ====================
@dp.callback_query(F.data == "admin_subscription_channels")
async def cb_admin_subscription_channels(cb: CallbackQuery):
    """Управление обязательными каналами"""
    if not is_admin(cb.from_user.id):
        return
    
    db = load_db()
    channels = db.get("required_channels", {})
    
    text = (
        f'{e("6039422865189638057", "📰")} <b>Обязательные каналы</b>\n\n'
        f'<blockquote>Текущих каналов: {len(channels)}</blockquote>\n'
    )
    
    for key, data in list(channels.items())[:5]:
        text += f'✓ <b>{data.get("name", key)}</b> (@{data.get("username", key)})\n'
    
    if len(channels) > 5:
        text += f'... и ещё {len(channels) - 5}'
    
    kb_rows = [
        [InlineKeyboardButton(text="➕ Добавить канал", callback_data="admin_add_channel")],
    ]
    
    for key in list(channels.keys())[:3]:
        kb_rows.append([InlineKeyboardButton(
            text=f"❌ {channels[key].get('name', key)}",
            callback_data=f"admin_remove_channel_{key}"
        )])
    
    kb_rows.append([InlineKeyboardButton(text="◁ Назад", callback_data="admin_panel")])
    
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@dp.callback_query(F.data == "admin_subscription_text")
async def cb_admin_subscription_text(cb: CallbackQuery, state: FSMContext):
    """Изменение текста подписки"""
    if not is_admin(cb.from_user.id):
        return
    
    db = load_db()
    current_text = db.get("subscription_text", "Для использования бота подпишитесь на наши каналы:")
    
    await state.set_state(AdminStates.edit_subscription_text)
    await cb.message.edit_text(
        f'{e("5870676941614354370", "🖋")} <b>Текст для проверки подписки</b>\n\n'
        f'<blockquote>Текущий текст:\n{current_text}\n\nВведите новый текст:</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_subscription_channels")]
        ])
    )
    await cb.answer()


@dp.message(AdminStates.edit_subscription_text)
async def process_subscription_text_edit(message: Message, state: FSMContext):
    """Обработка редактирования текста подписки"""
    if not is_admin(message.from_user.id):
        return
    
    db = load_db()
    db["subscription_text"] = message.text.strip()
    save_db(db)
    
    await state.clear()
    await message.answer(
        f'{e("5870633910337015697", "✅")} <b>Текст обновлён!</b>\n\n'
        f'<blockquote>Новый текст: {message.text.strip()}</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_subscription_channels")]
        ])
    )


@dp.callback_query(F.data == "admin_add_channel")
async def cb_admin_add_channel(cb: CallbackQuery, state: FSMContext):
    """Добавление нового канала - шаг 1: название"""
    if not is_admin(cb.from_user.id):
        return
    
    await state.set_state(AdminStates.add_channel_name)
    await cb.message.edit_text(
        f'{e("6039422865189638057", "📰")} <b>Добавить канал</b>\n\n'
        f'<blockquote>Шаг 1/2: Введите название канала\n\nНапример: <code>Stuffiny Dev</code></blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_subscription_channels")]
        ])
    )
    await cb.answer()


@dp.message(AdminStates.add_channel_name)
async def process_add_channel_name(message: Message, state: FSMContext):
    """Получаем название канала"""
    if not is_admin(message.from_user.id):
        return
    
    channel_name = message.text.strip()
    await state.update_data(channel_name=channel_name)
    await state.set_state(AdminStates.add_channel_url)
    
    await message.answer(
        f'{e("6039422865189638057", "📰")} <b>Добавить канал</b>\n\n'
        f'<blockquote>Шаг 2/2: Введите ссылку на канал\n\nНапример: <code>https://t.me/stuffinydev</code></blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_subscription_channels")]
        ])
    )


@dp.message(AdminStates.add_channel_url)
async def process_add_channel_url(message: Message, state: FSMContext):
    """Получаем ссылку на канал"""
    if not is_admin(message.from_user.id):
        return
    
    url = message.text.strip()
    
    # Проверяем формат ссылки
    if not url.startswith("https://t.me/") and not url.startswith("http://t.me/"):
        await message.answer(
            f'{e("5870657884844462243", "❌")} <b>Неверный формат ссылки!</b>\n\n'
            f'<blockquote>Ссылка должна начинаться с https://t.me/\n\nПопробуйте снова:</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Отмена", callback_data="admin_subscription_channels")]
            ])
        )
        return
    
    # Извлекаем username из ссылки
    username = url.replace("https://t.me/", "").replace("http://t.me/", "").split("?")[0].lower()
    
    data = await state.get_data()
    channel_name = data.get("channel_name", "Канал")
    
    db = load_db()
    if "required_channels" not in db:
        db["required_channels"] = {}
    
    db["required_channels"][username] = {
        "username": username,
        "name": channel_name,
        "url": url
    }
    save_db(db)
    
    await state.clear()
    await message.answer(
        f'{e("5870633910337015697", "✅")} <b>Канал добавлен!</b>\n\n'
        f'<blockquote>'
        f'<b>Название:</b> {channel_name}\n'
        f'<b>Ссылка:</b> {url}'
        f'</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_subscription_channels")]
        ])
    )


@dp.callback_query(F.data.startswith("admin_remove_channel_"))
async def cb_admin_remove_channel(cb: CallbackQuery):
    """Удаления канала"""
    if not is_admin(cb.from_user.id):
        return
    
    channel_key = cb.data.replace("admin_remove_channel_", "")
    db = load_db()
    
    if channel_key in db.get("required_channels", {}):
        del db["required_channels"][channel_key]
        save_db(db)
        await cb.answer(f"✅ Канал удалён", show_alert=True)
    else:
        await cb.answer(f"❌ Канал не найден", show_alert=True)
    
    await cb_admin_subscription_channels(cb)


# ==================== УСТАНОВКА ЦЕН НА СЕРВИСЫ ====================
@dp.callback_query(F.data == "admin_set_service_prices")
async def cb_admin_set_service_prices(cb: CallbackQuery):
    """Установка цен на сервисы - показываем список"""
    if not is_admin(cb.from_user.id):
        return
    
    msg = await cb.message.edit_text(
        f'{e("5345906554510012647", "🔄")} <b>Загружаю сервисы...</b>',
        reply_markup=None
    )
    
    # Загружаем все сервисы
    services_data = await api_get_services()
    
    if not services_data:
        await msg.edit_text(
            f'{e("5870657884844462243", "❌")} <b>Не удалось загрузить сервисы</b>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Повторить", callback_data="admin_set_service_prices")],
                [InlineKeyboardButton(text="Назад", callback_data="admin_panel")]
            ])
        )
        return
    
    db = load_db()
    custom_prices = db.get("custom_service_prices", {})
    
    # Конвертируем в список с ценами
    services = []
    for svc in services_data:
        if isinstance(svc, dict):
            code = svc.get("code", "")
            name = capitalize_service_name(svc.get("name", code))
            
            # Проверяем есть ли кастомная цена
            if code in custom_prices:
                price = float(custom_prices[code].get("price", 0))
            else:
                price = float(svc.get("cost", 0))
            
            services.append((code, name, price))
    
    # Сортируем по приоритету и названию
    services.sort(key=lambda x: (get_service_priority(x[0]), x[1]))
    
    # Показываем первую страницу
    await show_admin_services_page(msg, services, 0)
    await cb.answer()


async def show_admin_services_page(msg, services: list, page: int):
    """Показать страницу сервисов для установки цен"""
    per_page = 9  # 3x3 grid like buy_number
    total_pages = max(1, (len(services) + per_page - 1) // per_page)
    start = page * per_page
    end = start + per_page
    page_services = services[start:end]
    
    text = f'{e("5904462880941545555", "💰")} <b>Установить цены на сервисы</b>\n\n'
    text += f'<blockquote>Всего сервисов: {len(services)} | Страница {page + 1}/{total_pages}</blockquote>'
    
    # Создаем сетку кнопок 3x3 как в buy_number
    kb_rows = []
    row = []
    for code, name, price in page_services:
        btn = InlineKeyboardButton(
            text=f"{name}",
            callback_data=f"admin_set_price_{code}_{page}"
        )
        row.append(btn)
        if len(row) == 3:
            kb_rows.append(row)
            row = []
    if row:
        kb_rows.append(row)
    
    # Навигация
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="◁", callback_data=f"admin_services_page_{page - 1}"))
    nav_row.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="▷", callback_data=f"admin_services_page_{page + 1}"))
    kb_rows.append(nav_row)
    
    kb_rows.append([InlineKeyboardButton(text="◁ В админ панель", callback_data="admin_panel")])
    
    await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


@dp.callback_query(F.data.startswith("admin_services_page_"))
async def cb_admin_services_page(cb: CallbackQuery):
    """Переключение страниц сервисов для установки цен"""
    if not is_admin(cb.from_user.id):
        return
    
    page = int(cb.data.replace("admin_services_page_", ""))
    
    # Загружаем сервисы заново
    services_data = await api_get_services()
    
    if not services_data:
        await cb.answer("❌ Ошибка загрузки", show_alert=True)
        return
    
    db = load_db()
    custom_prices = db.get("custom_service_prices", {})
    
    services = []
    for svc in services_data:
        if isinstance(svc, dict):
            code = svc.get("code", "")
            name = capitalize_service_name(svc.get("name", code))
            
            if code in custom_prices:
                price = float(custom_prices[code].get("price", 0))
            else:
                price = float(svc.get("cost", 0))
            
            services.append((code, name, price))
    
    services.sort(key=lambda x: (get_service_priority(x[0]), x[1]))
    
    await show_admin_services_page(cb.message, services, page)
    await cb.answer()


@dp.callback_query(F.data.startswith("admin_set_price_"))
async def cb_admin_set_price(cb: CallbackQuery, state: FSMContext):
    """Установка цены на сервис - ввод цены"""
    if not is_admin(cb.from_user.id):
        return
    
    # Извлекаем service_code и page из callback_data
    parts = cb.data.replace("admin_set_price_", "").split("_")
    service_code = parts[0]
    page = int(parts[1]) if len(parts) > 1 else 0
    
    # Получаем информацию о сервисе
    services_data = await api_get_services()
    service_info = None
    for svc in services_data:
        if svc.get("code") == service_code:
            service_info = svc
            break
    
    if not service_info:
        await cb.answer("❌ Сервис не найден", show_alert=True)
        return
    
    service_name = service_info.get("name", service_code)
    
    # Проверяем текущую цену
    db = load_db()
    custom_prices = db.get("custom_service_prices", {})
    
    if service_code in custom_prices:
        current_price = custom_prices[service_code].get("price", 0)
    else:
        current_price = service_info.get("cost", 0)
    
    await state.set_state(AdminStates.edit_service_price)
    await state.update_data(service_code=service_code, service_name=service_name, return_page=page)
    
    await cb.message.edit_text(
        f'{e("5904462880941545555", "💰")} <b>Установить цену</b>\n\n'
        f'<blockquote>'
        f'<b>Сервис:</b> {service_name} (<code>{service_code}</code>)\n'
        f'<b>Текущая цена:</b> {current_price}₽\n\n'
        f'Введите новую цену (например: 15.5):'
        f'</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_set_service_prices")]
        ])
    )
    await cb.answer()


@dp.message(AdminStates.edit_service_price)
async def process_edit_service_price(message: Message, state: FSMContext):
    """Обработка установки цены на сервис"""
    if not is_admin(message.from_user.id):
        return
    
    data = await state.get_data()
    service_code = data.get("service_code")
    service_name = data.get("service_name")
    
    try:
        new_price = float(message.text.strip())
        if new_price < 0:
            raise ValueError("Цена не может быть отрицательной")
    except ValueError:
        await message.answer(
            f'{e("5870657884844462243", "❌")} <b>Неверный формат цены</b>\n\n'
            f'<blockquote>Введите число (например: 15.5):</blockquote>',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Отмена", callback_data="admin_set_service_prices")]
            ])
        )
        return
    
    # Сохраняем кастомную цену в базу данных
    db = load_db()
    if "custom_service_prices" not in db:
        db["custom_service_prices"] = {}
    
    db["custom_service_prices"][service_code] = {
        "name": service_name,
        "price": new_price,
        "updated_at": datetime.now().isoformat()
    }
    save_db(db)
    
    await state.clear()
    await message.answer(
        f'{e("5870633910337015697", "✅")} <b>Цена установлена!</b>\n\n'
        f'<blockquote>'
        f'<b>Сервис:</b> {service_name} (<code>{service_code}</code>)\n'
        f'<b>Новая цена:</b> {new_price}₽'
        f'</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад к списку", callback_data="admin_set_service_prices")],
            [InlineKeyboardButton(text="В админ панель", callback_data="admin_panel")]
        ])
    )


# ==================== ЗАПУСК ====================
async def main():
    # Инициализация тестовых каналов
    init_test_channels()
    
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
