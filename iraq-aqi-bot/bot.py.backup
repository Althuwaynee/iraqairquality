#!/usr/bin/env python3
# Iraq Air Quality Telegram Bot
# Location-based AQI alerts

import json
import time
import sqlite3
import requests
from math import radians, cos, sin, asin, sqrt
import logging

# Add at the top after imports
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Then modify your main() function:
def main():
    offset = 0
    logger.info("🤖 Bot starting...")
    print("🤖 Bot running...")  # Keep this too

    while True:
        try:
            logger.debug("Checking for updates...")
            updates = requests.get(
                f"{API}/getUpdates",
                params={"timeout": 30, "offset": offset}
            ).json()

            logger.debug(f"Got updates: {len(updates.get('result', []))}")
            
            for u in updates.get("result", []):
                offset = u["update_id"] + 1
                logger.info(f"Processing update: {u}")
# =======================
# CONFIG
# =======================
BOT_TOKEN = "8577945688:AAH91pjtaedisUE13rjxhoSiM7ZTXpPN0yk"
API = f"https://api.telegram.org/bot{BOT_TOKEN}"
DATA_FILE = "/home/omar/Documents/Dust/iraqairquality/data/pm10_alerts.json"
CHECK_INTERVAL = 300  # seconds (5 min)

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
  active INTEGER DEFAULT 1
)
""")
db.commit()

# =======================
# HELPERS
# =======================
def send_message(chat_id, text, keyboard=None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    if keyboard:
        payload["reply_markup"] = keyboard

    requests.post(f"{API}/sendMessage", json=payload)


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))


def is_dust_storm(pm10, aqi):
    return pm10 >= 300 or aqi >= 200


def load_data():
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)["districts"]

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

# =======================
# COMMAND HANDLERS
# =======================
def handle_start(chat_id):
    keyboard = {
        "keyboard": [[{"text": "📍 Share location", "request_location": True}]],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

    send_message(
        chat_id,
        "📍 شارك موقعك لتلقي تنبيهات جودة الهواء\n\n"
        "📍 Share your location to receive AQI alerts",
        keyboard
    )


def handle_location(chat_id, lat, lon):
    districts = load_data()

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

    cur.execute("""
    INSERT INTO users(chat_id, lat, lon, district_id)
    VALUES(?,?,?,?)
    ON CONFLICT(chat_id)
    DO UPDATE SET lat=?, lon=?, district_id=?, active=1
    """, (
        chat_id, lat, lon, nearest["district_id"],
        lat, lon, nearest["district_id"]
    ))
    db.commit()

    send_message(
        chat_id,
        f"✅ تم ربط موقعك مع:\n<b>{nearest['district_name']}</b>\n\n"
        f"ستصلك التنبيهات عند تغيّر جودة الهواء 🌫️"
    )


def handle_status(chat_id):
    row = cur.execute(
        "SELECT district_id, last_level, active FROM users WHERE chat_id=?",
        (chat_id,)
    ).fetchone()

    if not row:
        send_message(chat_id, "❌ لم يتم تسجيل موقعك بعد.")
        return

    status = "🟢 مفعّل" if row[2] else "🔕 متوقف"
    send_message(
        chat_id,
        f"📍 District ID: <b>{row[0]}</b>\n"
        f"Last AQI: <b>{row[1]}</b>\n"
        f"Status: {status}"
    )


def handle_stop(chat_id):
    cur.execute("UPDATE users SET active=0 WHERE chat_id=?", (chat_id,))
    db.commit()
    send_message(chat_id, "🔕 تم إيقاف التنبيهات.")


def handle_resume(chat_id):
    cur.execute("UPDATE users SET active=1 WHERE chat_id=?", (chat_id,))
    db.commit()
    send_message(chat_id, "🔔 تم تفعيل التنبيهات.")


def handle_lang(chat_id):
    row = cur.execute(
        "SELECT lang FROM users WHERE chat_id=?", (chat_id,)
    ).fetchone()

    new_lang = "en" if row and row[0] == "ar" else "ar"
    cur.execute("UPDATE users SET lang=? WHERE chat_id=?", (new_lang, chat_id))
    db.commit()

    send_message(chat_id, f"🌐 Language set to {new_lang.upper()}")

# =======================
# ALERT ENGINE
# =======================
def check_alerts():
    districts = load_data()
    users = cur.execute(
        "SELECT chat_id, lat, lon, last_level, lang FROM users WHERE active=1"
    ).fetchall()

    for chat_id, lat, lon, last, lang in users:
        nearest = None
        min_d = 99999

        for d in districts:
            dist = haversine(lat, lon, d["latitude"], d["longitude"])
            if dist < min_d:
                min_d = dist
                nearest = d

        level = nearest["alert"]["level"]
        aqi = nearest["aqi"]["value"]
        pm10 = nearest["pm10"]["now"]

        dust = is_dust_storm(pm10, aqi)

        if dust or level != last:
            msg = (
                f"📍 <b>{nearest['district_name']}</b>\n"
                f"AQI: <b>{aqi}</b>\n\n"
                f"{ALERT_TEXT[lang][level]}"
            )
            send_message(chat_id, msg)

            cur.execute(
                "UPDATE users SET last_level=? WHERE chat_id=?",
                (level, chat_id)
            )
            db.commit()

# =======================
# MAIN LOOP
# =======================
def main():
    offset = 0
    print("🤖 Bot running...")

    while True:
        updates = requests.get(
            f"{API}/getUpdates",
            params={"timeout": 30, "offset": offset}
        ).json()

        for u in updates.get("result", []):
            offset = u["update_id"] + 1

            msg = u.get("message")
            if not msg:
                continue

            chat_id = msg["chat"]["id"]

            if "location" in msg:
                loc = msg["location"]
                handle_location(chat_id, loc["latitude"], loc["longitude"])

            elif "text" in msg:
                text = msg["text"]

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

        check_alerts()
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()




