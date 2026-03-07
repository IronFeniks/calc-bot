import sqlite3
import os
import logging

logger = logging.getLogger(__name__)

PRICES_DB_PATH = 'data/prices.db'

def init_prices_db():
    """Создает таблицу для хранения цен, если её нет"""
    os.makedirs('data', exist_ok=True)
    
    conn = sqlite3.connect(PRICES_DB_PATH)
    cursor = conn.cursor()
    
    # Таблица для цен материалов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS material_prices (
            material_name TEXT PRIMARY KEY,
            price REAL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Таблица для цен чертежей (привязаны к изделию)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS drawing_prices (
            product_code TEXT,
            price REAL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (product_code)
        )
    ''')
    
    conn.commit()
    conn.close()
    logger.info("✅ База данных цен инициализирована")

def get_material_price(material_name):
    """Получает сохраненную цену материала"""
    try:
        conn = sqlite3.connect(PRICES_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT price FROM material_prices WHERE material_name = ?", (material_name,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else 0
    except Exception as e:
        logger.error(f"Ошибка получения цены материала: {e}")
        return 0

def save_material_price(material_name, price):
    """Сохраняет цену материала"""
    try:
        conn = sqlite3.connect(PRICES_DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO material_prices (material_name, price, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (material_name, price))
        conn.commit()
        conn.close()
        logger.info(f"💾 Сохранена цена материала {material_name}: {price}")
    except Exception as e:
        logger.error(f"Ошибка сохранения цены материала: {e}")

def get_drawing_price(product_code):
    """Получает сохраненную цену чертежа для изделия"""
    try:
        conn = sqlite3.connect(PRICES_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT price FROM drawing_prices WHERE product_code = ?", (product_code,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else 0
    except Exception as e:
        logger.error(f"Ошибка получения цены чертежа: {e}")
        return 0

def save_drawing_price(product_code, price):
    """Сохраняет цену чертежа для изделия"""
    try:
        conn = sqlite3.connect(PRICES_DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO drawing_prices (product_code, price, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (product_code, price))
        conn.commit()
        conn.close()
        logger.info(f"💾 Сохранена цена чертежа для {product_code}: {price}")
    except Exception as e:
        logger.error(f"Ошибка сохранения цены чертежа: {e}")

def get_all_material_prices():
    """Получает все сохраненные цены материалов"""
    try:
        conn = sqlite3.connect(PRICES_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT material_name, price FROM material_prices")
        rows = cursor.fetchall()
        conn.close()
        return {row[0]: row[1] for row in rows}
    except Exception as e:
        logger.error(f"Ошибка получения всех цен: {e}")
        return {}
