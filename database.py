import sqlite3
import os
import logging

logger = logging.getLogger(__name__)

# Путь к файлу базы данных
DB_PATH = 'data/calculator.db'

def get_connection():
    """Создает и возвращает соединение с базой данных"""
    os.makedirs('data', exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    """Проверяет наличие таблиц при запуске"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        logger.info(f"Найденные таблицы: {[t[0] for t in tables]}")
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка при проверке БД: {e}")

def get_all_categories():
    """Возвращает список всех уникальных категорий из таблицы с данными"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        # Используем правильное имя таблицы из вывода
        cursor.execute("SELECT DISTINCT Категории FROM \"___________________\" WHERE Категории IS NOT NULL AND Категории != ''")
        rows = cursor.fetchall()
        conn.close()
        categories = [row[0] for row in rows]
        logger.info(f"Найденные категории: {categories}")
        return categories
    except Exception as e:
        logger.error(f"Ошибка получения категорий: {e}")
        return []

def get_products_by_category(category):
    """Возвращает список изделий/узлов по категории"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT Код, Наименование FROM \"___________________\" 
            WHERE Тип IN ('изделие', 'узел') 
            AND Категории = ?
            ORDER BY Наименование
        """, (category,))
        rows = cursor.fetchall()
        conn.close()
        return [{'code': row[0], 'name': row[1]} for row in rows]
    except Exception as e:
        logger.error(f"Ошибка получения изделий: {e}")
        return []

def find_product_by_name(product_name):
    """Ищет изделие/узел по названию"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM \"___________________\" 
            WHERE Наименование = ? AND Тип IN ('изделие', 'узел')
        """, (product_name,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return dict(row)
        return None
    except Exception as e:
        logger.error(f"Ошибка поиска изделия: {e}")
        return None

def get_product_by_code(code):
    """Получает данные об изделии/узле/материале по коду"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM \"___________________\" WHERE Код = ?", (code,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return dict(row)
        return None
    except Exception as e:
        logger.error(f"Ошибка получения данных по коду: {e}")
        return None

def get_materials_for_product(product_code):
    """Получает все материалы для изделия/узла из таблицы спецификаций"""
    try:
        materials = []
        # Пока спецификации пусты, возвращаем пустой список
        return materials
    except Exception as e:
        logger.error(f"Ошибка получения материалов: {e}")
        return []
