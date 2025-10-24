import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes

# === CARGAR VARIABLES DESDE ENTORNO ===
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("❌ TELEGRAM_TOKEN o TELEGRAM_CHAT_ID no están definidos. Verifica tu .bashrc y usa 'source ~/.bashrc'.")

# === COMANDOS ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 ¡Hola! Soy tu bot de Telegram. ¿En qué puedo ayudarte hoy?")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📚 Comandos disponibles:\n/start - Iniciar bot\n/help - Ver ayuda\n/alerta - Enviar alerta")

async def alerta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="🚨 Alerta enviada desde el bot.")

async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"📨 Recibido: {update.message.text}")

# === INICIALIZACIÓN DEL BOT ===
def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("alerta", alerta))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

    print("✅ Bot iniciado...")
    app.run_polling()

if __name__ == "__main__":
    main()

