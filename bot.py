import asyncio
import logging
import os
import sqlite3
from datetime import datetime, time
from zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv
from telegram import (InlineKeyboardButton, InlineKeyboardMarkup,
                      KeyboardButton, ReplyKeyboardMarkup,
                      ReplyKeyboardRemove, Update)
from telegram.error import Forbidden
from telegram.ext import (Application, CallbackQueryHandler, CommandHandler,
                          ContextTypes, ConversationHandler, MessageHandler,
                          filters)

load_dotenv()
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("weatherbot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
MORNING = os.getenv("MORNING_TIME", "07:00")
TZ = ZoneInfo("Asia/Tashkent")
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.db")

GEO_URL = "https://geocoding-api.open-meteo.com/v1/search"
WX_URL = "https://api.open-meteo.com/v1/forecast"

# Viloyat markazlari (lat, lon)
REGIONS = [
    ("Toshkent shahri", 41.2995, 69.2401),
    ("Toshkent viloyati", 41.0167, 69.3600),
    ("Andijon", 40.7821, 72.3442),
    ("Buxoro", 39.7747, 64.4286),
    ("Farg'ona", 40.3842, 71.7843),
    ("Jizzax", 40.1158, 67.8422),
    ("Xorazm", 41.5500, 60.6333),
    ("Namangan", 40.9983, 71.6726),
    ("Navoiy", 40.0844, 65.3792),
    ("Qashqadaryo", 38.8606, 65.7891),
    ("Qoraqalpog'iston", 42.4531, 59.6103),
    ("Samarqand", 39.6270, 66.9750),
    ("Sirdaryo", 40.4897, 68.7842),
    ("Surxondaryo", 37.2242, 67.2783),
]

CODES = {
    0: ("Ochiq", "☀️"), 1: ("Asosan ochiq", "🌤"), 2: ("Qisman bulutli", "⛅"),
    3: ("Bulutli", "☁️"), 45: ("Tumanli", "🌫"), 48: ("Qirovli tuman", "🌫"),
    51: ("Mayda yog'in", "🌦"), 53: ("Yengil yomg'ir", "🌦"), 55: ("Yomg'ir", "🌧"),
    56: ("Muzlovchi shivalash", "🌧"), 57: ("Muzlovchi yomg'ir", "🌧"),
    61: ("Yengil yomg'ir", "🌦"), 63: ("Yomg'ir", "🌧"), 65: ("Kuchli yomg'ir", "🌧"),
    66: ("Muzlovchi yomg'ir", "🌧"), 67: ("Kuchli muzlovchi yomg'ir", "🌧"),
    71: ("Yengil qor", "🌨"), 73: ("Qor", "❄️"), 75: ("Kuchli qor", "❄️"),
    77: ("Qor donalari", "❄️"), 80: ("Jala", "🌦"), 81: ("Kuchli jala", "🌧"),
    82: ("Juda kuchli jala", "⛈"), 85: ("Qor yog'ishi", "🌨"),
    86: ("Kuchli qor yog'ishi", "❄️"), 95: ("Momaqaldiroq", "⛈"),
    96: ("Momaqaldiroq va do'l", "⛈"), 99: ("Kuchli do'l bilan momaqaldiroq", "⛈"),
}

WAIT_BROADCAST = 1


# ---------------------------------------------------------------- DB
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as c:
        c.execute(
            """CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY,
                first_name TEXT,
                username TEXT,
                place TEXT,
                lat REAL,
                lon REAL,
                active INTEGER DEFAULT 1,
                joined TEXT
            )"""
        )


def upsert_user(user):
    with db() as c:
        c.execute(
            """INSERT INTO users (chat_id, first_name, username, joined)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(chat_id) DO UPDATE SET
                 first_name=excluded.first_name,
                 username=excluded.username,
                 active=1""",
            (user.id, user.first_name, user.username, datetime.now(TZ).isoformat()),
        )


def set_location(chat_id, place, lat, lon):
    with db() as c:
        c.execute(
            "UPDATE users SET place=?, lat=?, lon=?, active=1 WHERE chat_id=?",
            (place, lat, lon, chat_id),
        )


def get_user(chat_id):
    with db() as c:
        return c.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,)).fetchone()


def set_active(chat_id, flag):
    with db() as c:
        c.execute("UPDATE users SET active=? WHERE chat_id=?", (flag, chat_id))


def active_users():
    with db() as c:
        return c.execute("SELECT * FROM users WHERE active=1").fetchall()


def stats():
    with db() as c:
        total = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        active = c.execute("SELECT COUNT(*) FROM users WHERE active=1").fetchone()[0]
        with_loc = c.execute(
            "SELECT COUNT(*) FROM users WHERE lat IS NOT NULL AND active=1"
        ).fetchone()[0]
        today = c.execute(
            "SELECT COUNT(*) FROM users WHERE joined >= ?",
            (datetime.now(TZ).strftime("%Y-%m-%d"),),
        ).fetchone()[0]
    return total, active, with_loc, today


# ---------------------------------------------------------------- Weather
async def geocode(name: str):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            GEO_URL,
            params={"name": name, "count": 5, "language": "en", "country_code": "UZ"},
        )
        r.raise_for_status()
    return r.json().get("results") or []


async def fetch_weather(lat: float, lon: float) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,apparent_temperature,relative_humidity_2m,"
                   "weather_code,wind_speed_10m",
        "daily": "weather_code,temperature_2m_max,temperature_2m_min,"
                 "precipitation_sum,precipitation_probability_max",
        "wind_speed_unit": "ms",
        "timezone": "Asia/Tashkent",
        "forecast_days": 1,
    }
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(WX_URL, params=params)
        r.raise_for_status()
    return r.json()


def format_weather(place: str, data: dict) -> str:
    cur, day = data["current"], data["daily"]
    desc, icon = CODES.get(day["weather_code"][0], ("Noma'lum", "🌡"))
    cdesc, cicon = CODES.get(cur["weather_code"], (desc, icon))
    prob = day["precipitation_probability_max"][0]
    prob_txt = f" (ehtimol {prob}%)" if prob is not None else ""
    return (
        f"📍 {place}\n"
        f"{cicon} Hozir: {cdesc}\n"
        f"🌡 Harorat: {cur['temperature_2m']:.0f}°C "
        f"(his qilinishi {cur['apparent_temperature']:.0f}°C)\n"
        f"📈 Bugun: {day['temperature_2m_min'][0]:.0f}° … "
        f"{day['temperature_2m_max'][0]:.0f}°C, {icon} {desc}\n"
        f"🌧 Yog'ingarchilik: {day['precipitation_sum'][0]:.1f} mm{prob_txt}\n"
        f"💧 Namlik: {cur['relative_humidity_2m']}%\n"
        f"💨 Shamol: {cur['wind_speed_10m']:.1f} m/s"
    )


async def build_report(place, lat, lon) -> str:
    return format_weather(place, await fetch_weather(lat, lon))


# ---------------------------------------------------------------- Handlers
def region_keyboard():
    rows, row = [], []
    for i, (name, _, _) in enumerate(REGIONS):
        row.append(InlineKeyboardButton(name, callback_data=f"reg:{i}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


LOC_KB = ReplyKeyboardMarkup(
    [[KeyboardButton("📍 Joylashuvimni yuborish", request_location=True)]],
    resize_keyboard=True,
    one_time_keyboard=True,
)


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    upsert_user(update.effective_user)
    await update.message.reply_text(
        f"Assalomu alaykum, {update.effective_user.first_name}! 👋\n"
        "Men O'zbekiston bo'yicha ob-havo botiman. Har kuni ertalab "
        f"{MORNING} da ob-havoni yuboraman.\n\nViloyatingizni tanlang:",
        reply_markup=region_keyboard(),
    )


async def region_chosen(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    name, lat, lon = REGIONS[int(q.data.split(":")[1])]
    upsert_user(q.from_user)
    set_location(q.from_user.id, name, lat, lon)
    await q.edit_message_text(f"✅ {name} tanlandi.")
    await ctx.bot.send_message(
        q.from_user.id,
        "Aniqroq natija uchun tuman/shahar nomini yozing (masalan: Chirchiq, "
        "Kattaqo'rg'on) yoki 📍 joylashuvingizni yuboring.\n\n"
        "Viloyat markazi uchun hozirgi ob-havo: /weather",
        reply_markup=LOC_KB,
    )


async def place_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    upsert_user(update.effective_user)
    name = update.message.text.strip()
    try:
        results = await geocode(name)
    except Exception:
        log.exception("geocode failed")
        await update.message.reply_text("Qidirishda xatolik. Keyinroq urinib ko'ring.")
        return
    if not results:
        await update.message.reply_text(
            "Topilmadi 😕 Lotin harflarida yozib ko'ring (masalan: Chirchiq) "
            "yoki joylashuv yuboring."
        )
        return
    r = results[0]
    place = r["name"] + (f", {r['admin1']}" if r.get("admin1") else "")
    set_location(update.effective_user.id, place, r["latitude"], r["longitude"])
    await update.message.reply_text(
        f"✅ Joy saqlandi: {place}", reply_markup=ReplyKeyboardRemove()
    )
    await send_weather(update.effective_chat.id, ctx)


async def location_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    upsert_user(update.effective_user)
    loc = update.message.location
    set_location(update.effective_user.id, "Joylashuvingiz", loc.latitude, loc.longitude)
    await update.message.reply_text(
        "✅ Joylashuv saqlandi.", reply_markup=ReplyKeyboardRemove()
    )
    await send_weather(update.effective_chat.id, ctx)


async def send_weather(chat_id: int, ctx: ContextTypes.DEFAULT_TYPE):
    u = get_user(chat_id)
    if not u or u["lat"] is None:
        await ctx.bot.send_message(chat_id, "Avval joyni tanlang: /start")
        return
    try:
        text = await build_report(u["place"], u["lat"], u["lon"])
    except Exception:
        log.exception("weather failed")
        text = "Ob-havo ma'lumotini olib bo'lmadi. Keyinroq urinib ko'ring."
    await ctx.bot.send_message(chat_id, text)


async def weather_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await send_weather(update.effective_chat.id, ctx)


async def stop_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    set_active(update.effective_user.id, 0)
    await update.message.reply_text(
        "Ertalabki xabarlar o'chirildi. Qayta yoqish: /start"
    )


async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/start – joyni tanlash\n/weather – hozirgi ob-havo\n"
        "/stop – ertalabki xabarni o'chirish"
    )


# ---------------------------------------------------------------- Morning job
async def morning_job(ctx: ContextTypes.DEFAULT_TYPE):
    cache: dict = {}
    sent = 0
    for u in active_users():
        if u["lat"] is None:
            continue
        key = (round(u["lat"], 2), round(u["lon"], 2))
        try:
            if key not in cache:
                cache[key] = await fetch_weather(u["lat"], u["lon"])
            text = "🌅 Xayrli tong!\n\n" + format_weather(u["place"], cache[key])
            await ctx.bot.send_message(u["chat_id"], text)
            sent += 1
        except Forbidden:
            set_active(u["chat_id"], 0)
        except Exception:
            log.exception("morning send failed for %s", u["chat_id"])
        await asyncio.sleep(0.05)  # Telegram limitiga tushmaslik uchun
    log.info("Morning weather sent to %s users", sent)


# ---------------------------------------------------------------- Admin
def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


async def admin_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    total, active, with_loc, today = stats()
    await update.message.reply_text(
        "🛠 Admin panel\n\n"
        f"👥 Jami foydalanuvchilar: {total}\n"
        f"✅ Faol: {active}\n"
        f"📍 Joyi saqlanganlar: {with_loc}\n"
        f"🆕 Bugun qo'shilgan: {today}\n\n"
        "/broadcast – hammaga xabar yuborish"
    )


async def broadcast_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await update.message.reply_text(
        "Yuboriladigan xabarni yuboring (matn, rasm, ...).\nBekor qilish: /cancel"
    )
    return WAIT_BROADCAST


async def broadcast_send(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    ok = fail = 0
    status = await msg.reply_text("Yuborilmoqda...")
    for u in active_users():
        try:
            await ctx.bot.copy_message(u["chat_id"], msg.chat_id, msg.message_id)
            ok += 1
        except Forbidden:
            set_active(u["chat_id"], 0)
            fail += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await status.edit_text(f"✅ Yuborildi: {ok}\n❌ Xatolik/bloklagan: {fail}")
    return ConversationHandler.END


async def broadcast_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Bekor qilindi.")
    return ConversationHandler.END


# ---------------------------------------------------------------- Main
async def on_error(update, ctx: ContextTypes.DEFAULT_TYPE):
    log.error("Unhandled error", exc_info=ctx.error)


def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(
        ConversationHandler(
            entry_points=[CommandHandler("broadcast", broadcast_start)],
            states={
                WAIT_BROADCAST: [
                    CommandHandler("cancel", broadcast_cancel),
                    MessageHandler(~filters.COMMAND, broadcast_send),
                ]
            },
            fallbacks=[CommandHandler("cancel", broadcast_cancel)],
        )
    )
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("weather", weather_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CallbackQueryHandler(region_chosen, pattern=r"^reg:\d+$"))
    app.add_handler(MessageHandler(filters.LOCATION, location_msg))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, place_text))
    app.add_error_handler(on_error)

    hh, mm = (int(x) for x in MORNING.split(":"))
    app.job_queue.run_daily(morning_job, time=time(hh, mm, tzinfo=TZ), name="morning")

    log.info("Bot ishga tushdi")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
