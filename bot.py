import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import config

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def check_access(update: Update) -> bool:
    """Проверка доступа"""
    chat_id = update.effective_chat.id
    topic_id = update.message.message_thread_id if update.message else None
    
    if chat_id == config.GROUP_ID and topic_id == config.TOPIC_ID:
        return True
    
    logger.warning(f"Доступ запрещен: chat={chat_id}, topic={topic_id}")
    return False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка /start"""
    if not await check_access(update):
        return
    
    await update.message.reply_text("✅ Бот успешно запущен через GitHub!")

def main():
    """Запуск бота"""
    app = Application.builder().token(config.TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    
    logger.info("Бот запускается...")
    app.run_polling()

if __name__ == "__main__":
    main()
