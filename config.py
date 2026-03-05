import os
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# Токен бота
TOKEN = os.getenv('TOKEN')
if not TOKEN:
    raise ValueError("TOKEN не задан в .env файле")

# ID группы и темы
GROUP_ID = int(os.getenv('GROUP_ID', 0))
TOPIC_ID = int(os.getenv('TOPIC_ID', 0))

# Ссылка на Яндекс Таблицу
YANDEX_TABLE_URL = os.getenv('YANDEX_TABLE_URL')

# Настройки кэширования
CACHE_TTL = int(os.getenv('CACHE_TTL', 300))
