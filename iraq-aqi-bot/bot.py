from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes
)

BOT_TOKEN = "8577945688:AAFAZ6P9GMXhc2ch2aMOhs-9i3yDFMpxHRo"

# ---------------------------
# Commands
# ---------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🌍 مرحباً!\n\n"
        "هذا البوت يرسل تنبيهات الغبار وجودة الهواء حسب موقعك.\n\n"
        "الأوامر المتاحة:\n"
        "/location – تحديد موقعك\n"
        "/stop – إيقاف التنبيهات"
    )

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🛑 تم إيقاف التنبيهات.\n"
        "يمكنك العودة في أي وقت باستخدام /start"
    )

async def location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📍 أرسل موقعك الآن باستخدام زر 📎 → الموقع\n"
        "وسنحدد أقرب منطقة لك تلقائياً."
    )

# ---------------------------
# Main
# ---------------------------

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CommandHandler("location", location))

    print("🤖 Bot is running...")
    app.run_polling()

if __name__ == "__main__":
    main()
