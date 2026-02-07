#!/usr/bin/env python3
# Iraq Air Quality Telegram Bot
# Location-based AQI alerts

import json
import time
import sqlite3
import requests
import logging
from math import radians, cos, sin, asin, sqrt

# =======================
# LOGGING
# =======================
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =======================
# CONFIG
# =======================
BOT_TOKEN = "8577945688:AAH91pjtaedisUE13rjxhoSiM7ZTXpPN0yk"
API = f"https://api.telegram.org/bot{BOT_TOKEN}"
DATA_FILE = "/home/omar/Documents/Dust/iraqairquality/data/pm10_alerts.json"
CHECK_INTERVAL = 2  # seconds (5 min)

# =======================
# DATABASE
# =======================
db = sqlite3.connect("users.db", check_same_thread=False)
cur = db.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS users (
  chat_id INTEGER PRIMARY KEY,
  lat REAL,
  lon REAL,
  district_id TEXT,
  last_level TEXT,
  lang TEXT DEFAULT 'ar',
  active INTEGER DEFAULT 1,
  last_alert TEXT DEFAULT NULL
)
""")
db.commit()

# =======================
# HELPERS
# =======================
def send_message(chat_id, text, keyboard=None):
    try:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        if keyboard:
            payload["reply_markup"] = keyboard

        response = requests.post(f"{API}/sendMessage", json=payload, timeout=10)
        logger.debug(f"Sent message to {chat_id}: {text[:50]}...")
        return response
    except Exception as e:
        logger.error(f"Failed to send message to {chat_id}: {e}")
        return None


def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance between two points on Earth."""
    R = 6371  # Earth's radius in km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))


def is_dust_storm(pm10, aqi):
    """Check if conditions indicate a dust storm."""
    return pm10 >= 300 or aqi >= 200


def load_data():
    """Load air quality data from JSON file."""
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            logger.debug(f"Loaded data for {len(data.get('districts', []))} districts")
            return data["districts"]
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        return []

# =======================
# TEXT (AR / EN)
# =======================
ALERT_TEXT = {
    "ar": {
        "good": "✅ جودة الهواء جيدة.\n🌳 يمكنك الخروج بأمان.",
        "moderate": "😐 جودة الهواء متوسطة.\nالخروج ممكن مع الحذر.",
        "unhealthy_for_sensitive_groups":
            "⚠️ غير صحي للفئات الحساسة.\n👶 يُفضّل البقاء في الداخل.",
        "unhealthy":
            "🚨 جودة الهواء غير صحية.\n😷 ارتدِ كمامة.",
        "very_unhealthy":
            "☠️ سيئة جداً.\n🏠 ابقَ في الداخل.",
        "hazardous":
            "🌪️ حالة خطيرة!\n🚫 تجنب الخروج تماماً."
    },
    "en": {
        "good": "✅ Air quality is good.\n🌳 You can go outside safely.",
        "moderate": "😐 Air quality is moderate.",
        "unhealthy_for_sensitive_groups":
            "⚠️ Unhealthy for sensitive groups.",
        "unhealthy":
            "🚨 Unhealthy air quality.\n😷 Wear a mask.",
        "very_unhealthy":
            "☠️ Very unhealthy.\n🏠 Stay indoors.",
        "hazardous":
            "🌪️ Hazardous conditions!\n🚫 Avoid going outside."
    }
}

LEVEL_EMOJI = {
    "good": "✅",
    "moderate": "😐",
    "unhealthy_for_sensitive_groups": "⚠️",
    "unhealthy": "🚨",
    "very_unhealthy": "☠️",
    "hazardous": "🌪️"
}

# =======================
# COMMAND HANDLERS
# =======================
def handle_start(chat_id):
    """Send welcome message with location request."""
    logger.info(f"New user: {chat_id}")
    
    keyboard = {
        "keyboard": [[{"text": "📍 Share location", "request_location": True}]],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

    send_message(
        chat_id,
        "مرحباً! 👋\n"
        "📍 شارك موقعك لتلقي تنبيهات جودة الهواء\n\n"
        "Welcome! 👋\n"
        "📍 Share your location to receive AQI alerts",
        keyboard
    )


def handle_location(chat_id, lat, lon):
    """Save user's location and find nearest district."""
    logger.info(f"Received location from {chat_id}: lat={lat}, lon={lon}")
    
    districts = load_data()
    
    if not districts:
        send_message(chat_id, "❌ Could not load air quality data. Please try again later.")
        return

    nearest = None
    min_d = 99999

    for d in districts:
        if not d.get("latitude"):
            continue
        dist = haversine(lat, lon, d["latitude"], d["longitude"])
        if dist < min_d:
            min_d = dist
            nearest = d

    if not nearest:
        send_message(chat_id, "❌ Could not determine nearest district.")
        return

    logger.info(f"Nearest district for {chat_id}: {nearest['district_name']} (ID: {nearest['district_id']})")
    
    cur.execute("""
    INSERT INTO users(chat_id, lat, lon, district_id, last_level)
    VALUES(?,?,?,?,?)
    ON CONFLICT(chat_id)
    DO UPDATE SET lat=?, lon=?, district_id=?, last_level=?, active=1
    """, (
        chat_id, lat, lon, nearest["district_id"], nearest["alert"]["level"],
        lat, lon, nearest["district_id"], nearest["alert"]["level"]
    ))
    db.commit()

    send_message(
        chat_id,
        f"✅ تم ربط موقعك مع:\n<b>{nearest['district_name']}</b>\n\n"
        f"ستصلك التنبيهات عند تغيّر جودة الهواء 🌫️\n\n"
        f"Use /current to check air quality now!"
    )


def handle_status(chat_id):
    """Show user's current status and settings."""
    row = cur.execute(
        "SELECT district_id, last_level, active, lat, lon, lang FROM users WHERE chat_id=?",
        (chat_id,)
    ).fetchone()

    if not row:
        send_message(chat_id, "❌ لم يتم تسجيل موقعك بعد. Use /start to begin.")
        return

    district_id, last_level, active, lat, lon, lang = row
    status = "🟢 مفعّل" if active else "🔕 متوقف"
    
    # Try to get district name
    districts = load_data()
    district_name = district_id
    for d in districts:
        if d.get("district_id") == district_id:
            district_name = d.get("district_name", district_id)
            break

    if lang == "ar":
        message = (
            f"📊 <b>حالتك الحالية</b>\n\n"
            f"📍 الموقع: <b>{district_name}</b>\n"
            f"📌 كود المنطقة: {district_id}\n"
            f"🌐 الإحداثيات: {lat:.4f}, {lon:.4f}\n"
            f"📈 آخر مستوى: <b>{last_level}</b>\n"
            f"🔔 الحالة: {status}\n"
            f"🌐 اللغة: {'عربي' if lang == 'ar' else 'إنجليزي'}\n\n"
            f"استخدم /current للتحقق من جودة الهواء الآن"
        )
    else:
        message = (
            f"📊 <b>Your Current Status</b>\n\n"
            f"📍 Location: <b>{district_name}</b>\n"
            f"📌 District ID: {district_id}\n"
            f"🌐 Coordinates: {lat:.4f}, {lon:.4f}\n"
            f"📈 Last level: <b>{last_level}</b>\n"
            f"🔔 Status: {status}\n"
            f"🌐 Language: {'Arabic' if lang == 'ar' else 'English'}\n\n"
            f"Use /current to check air quality now"
        )
    
    send_message(chat_id, message)


def handle_stop(chat_id):
    """Stop alerts for user."""
    cur.execute("UPDATE users SET active=0 WHERE chat_id=?", (chat_id,))
    db.commit()
    logger.info(f"User {chat_id} stopped alerts")
    send_message(chat_id, "🔕 تم إيقاف التنبيهات. Use /resume to start again.")


def handle_resume(chat_id):
    """Resume alerts for user."""
    cur.execute("UPDATE users SET active=1 WHERE chat_id=?", (chat_id,))
    db.commit()
    logger.info(f"User {chat_id} resumed alerts")
    send_message(chat_id, "🔔 تم تفعيل التنبيهات.")


def handle_lang(chat_id):
    """Switch language between Arabic and English."""
    row = cur.execute(
        "SELECT lang FROM users WHERE chat_id=?", (chat_id,)
    ).fetchone()

    if not row:
        new_lang = "ar"
    else:
        new_lang = "en" if row[0] == "ar" else "ar"
    
    cur.execute("UPDATE users SET lang=? WHERE chat_id=?", (new_lang, chat_id))
    db.commit()
    
    logger.info(f"User {chat_id} switched language to {new_lang}")
    
    if new_lang == "ar":
        send_message(chat_id, "🌐 تم تغيير اللغة إلى العربية")
    else:
        send_message(chat_id, "🌐 Language switched to English")


def handle_current(chat_id):
    """Send current air quality for user's location."""
    logger.info(f"Current AQI requested by {chat_id}")
    
    # Get user's location
    row = cur.execute(
        "SELECT lat, lon, district_id, lang FROM users WHERE chat_id=?",
        (chat_id,)
    ).fetchone()

    if not row:
        send_message(chat_id, "❌ Please share your location first with /start")
        return

    lat, lon, district_id, lang = row
    
    # Load data and find nearest district
    districts = load_data()
    
    if not districts:
        send_message(chat_id, "❌ Could not load air quality data. Please try again later.")
        return
    
    nearest = None
    min_d = 99999

    for d in districts:
        if not d.get("latitude"):
            continue
        dist = haversine(lat, lon, d["latitude"], d["longitude"])
        if dist < min_d:
            min_d = dist
            nearest = d

    if not nearest:
        send_message(chat_id, "❌ Could not find air quality data for your area")
        return

    # Prepare response
    level = nearest["alert"]["level"]
    aqi = nearest["aqi"]["value"]
    pm10 = nearest["pm10"]["now"]
    
    # Get level description
    level_desc = ALERT_TEXT[lang].get(level, level)
    
    # Get emoji
    emoji = LEVEL_EMOJI.get(level, "📊")
    
    # Prepare message
    if lang == "ar":
        message = (
            f"{emoji} <b>جودة الهواء الحالية</b>\n"
            f"📍 <b>{nearest['district_name']}</b>\n"
            f"📏 المسافة: {min_d:.1f} كم\n\n"
            f"📊 مؤشر جودة الهواء (AQI): <b>{aqi}</b>\n"
            f"🏭 PM10: <b>{int(pm10)} µg/m³</b>\n"
            f"📈 المستوى: <b>{level}</b>\n\n"
            f"{level_desc}\n\n"
            f"⚠️ تنبيهات الغبار: {'نعم' if is_dust_storm(pm10, aqi) else 'لا'}"
        )
    else:
        message = (
            f"{emoji} <b>Current Air Quality</b>\n"
            f"📍 <b>{nearest['district_name']}</b>\n"
            f"📏 Distance: {min_d:.1f} km\n\n"
            f"📊 Air Quality Index (AQI): <b>{aqi}</b>\n"
            f"🏭 PM10: <b>{int(pm10)} µg/m³</b>\n"
            f"📈 Level: <b>{level}</b>\n\n"
            f"{level_desc}\n\n"
            f"⚠️ Dust storm alert: {'YES' if is_dust_storm(pm10, aqi) else 'No'}"
        )
    
    send_message(chat_id, message)
    
    # Update last level
    cur.execute(
        "UPDATE users SET last_level=? WHERE chat_id=?",
        (level, chat_id)
    )
    db.commit()


def handle_help(chat_id):
    """Show help message with available commands."""
    help_text = (
        "<b>🤖 Iraq Air Quality Bot</b>\n\n"
        "<b>Available Commands:</b>\n"
        "/start - Share location and start bot\n"
        "/current - Check current air quality\n"
        "/status - Check your subscription status\n"
        "/stop - Stop receiving alerts\n"
        "/resume - Resume alerts\n"
        "/lang - Switch language (AR/EN)\n"
        "/help - Show this help message\n\n"
        "📍 The bot will send automatic alerts when air quality changes."
    )
    send_message(chat_id, help_text)

# =======================
# ALERT ENGINE
# =======================
def check_alerts():
    """Check for air quality changes and send alerts."""
    try:
        districts = load_data()
        
        if not districts:
            logger.warning("No districts data available")
            return
        
        users = cur.execute(
            "SELECT chat_id, lat, lon, last_level, lang FROM users WHERE active=1"
        ).fetchall()
        
        logger.debug(f"Checking alerts for {len(users)} active users")
        
        for chat_id, lat, lon, last_level, lang in users:
            try:
                nearest = None
                min_d = 99999

                for d in districts:
                    if not d.get("latitude") or not d.get("longitude"):
                        continue
                    dist = haversine(lat, lon, d["latitude"], d["longitude"])
                    if dist < min_d:
                        min_d = dist
                        nearest = d

                if not nearest:
                    logger.warning(f"No nearest district found for user {chat_id}")
                    continue

                level = nearest["alert"]["level"]
                aqi = nearest["aqi"]["value"]
                pm10 = nearest["pm10"]["now"]

                # Check if level exists in ALERT_TEXT
                if level not in ALERT_TEXT[lang]:
                    logger.warning(f"Unknown level '{level}' for lang '{lang}'")
                    continue

                dust = is_dust_storm(pm10, aqi)

                # Send alert if:
                # 1. Dust storm detected, OR
                # 2. Air quality level changed, OR
                # 3. First alert (last_level is NULL)
                if dust or level != last_level or last_level is None:
                    logger.info(f"Sending alert to {chat_id}: {level} (prev: {last_level}, dust: {dust})")
                    
                    emoji = LEVEL_EMOJI.get(level, "📊")
                    
                    if lang == "ar":
                        msg = (
                            f"{emoji} <b>تنبيه جودة الهواء</b>\n"
                            f"📍 <b>{nearest['district_name']}</b>\n\n"
                            f"📊 مؤشر جودة الهواء (AQI): <b>{aqi}</b>\n"
                            f"🏭 PM10: <b>{pm10} µg/m³</b>\n"
                            f"📈 المستوى: <b>{level}</b>\n\n"
                            f"{ALERT_TEXT[lang][level]}"
                        )
                    else:
                        msg = (
                            f"{emoji} <b>Air Quality Alert</b>\n"
                            f"📍 <b>{nearest['district_name']}</b>\n\n"
                            f"📊 Air Quality Index (AQI): <b>{aqi}</b>\n"
                            f"🏭 PM10: <b>{pm10} µg/m³</b>\n"
                            f"📈 Level: <b>{level}</b>\n\n"
                            f"{ALERT_TEXT[lang][level]}"
                        )
                    
                    send_message(chat_id, msg)

                    # Update last level in database
                    cur.execute(
                        "UPDATE users SET last_level=? WHERE chat_id=?",
                        (level, chat_id)
                    )
                    db.commit()
                    
            except Exception as e:
                logger.error(f"Error processing user {chat_id}: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error in check_alerts: {e}")

# =======================
# MAIN LOOP
# =======================
def main():
    offset = 0
    logger.info("🤖 Bot starting...")
    
    # Test data loading
    districts = load_data()
    logger.info(f"Loaded {len(districts)} districts")
    
    # Test bot token
    try:
        me = requests.get(f"{API}/getMe", timeout=10).json()
        if me.get("ok"):
            logger.info(f"Bot connected: @{me['result']['username']}")
        else:
            logger.error(f"Bot token error: {me}")
    except Exception as e:
        logger.error(f"Failed to connect to Telegram API: {e}")

    while True:
        try:
            # Get updates from Telegram
            logger.debug("Getting updates from Telegram...")
            response = requests.get(
                f"{API}/getUpdates",
                params={"timeout": 30, "offset": offset},
                timeout=35
            )
            updates = response.json()
            
            if not updates.get("ok"):
                logger.error(f"Telegram API error: {updates}")
                time.sleep(5)
                continue
            
            updates_list = updates.get("result", [])
            logger.debug(f"Received {len(updates_list)} updates")
            
            for u in updates_list:
                offset = u["update_id"] + 1
                logger.debug(f"Processing update {u['update_id']}")
                
                msg = u.get("message")
                if not msg:
                    continue

                chat_id = msg["chat"]["id"]
                
                # Handle location
                if "location" in msg:
                    loc = msg["location"]
                    handle_location(chat_id, loc["latitude"], loc["longitude"])
                
                # Handle text commands
                elif "text" in msg:
                    text = msg["text"]
                    logger.info(f"Command from {chat_id}: {text}")
                    
                    if text == "/start":
                        handle_start(chat_id)
                    elif text == "/status":
                        handle_status(chat_id)
                    elif text == "/stop":
                        handle_stop(chat_id)
                    elif text == "/resume":
                        handle_resume(chat_id)
                    elif text == "/lang":
                        handle_lang(chat_id)
                    elif text == "/current":
                        handle_current(chat_id)
                    elif text == "/help":
                        handle_help(chat_id)
                    elif text.startswith("/"):
                        # Unknown command
                        send_message(chat_id, "❌ Unknown command. Use /help to see available commands.")
            
            # Check for alerts
            check_alerts()
            
            # Wait before next check
            time.sleep(CHECK_INTERVAL)
            
        except requests.exceptions.Timeout:
            logger.warning("Request timeout, retrying...")
            time.sleep(5)
        except requests.exceptions.ConnectionError:
            logger.error("Connection error, retrying in 10 seconds...")
            time.sleep(10)
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()