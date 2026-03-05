# database.py - модуль для работы с базой данных SQLite
# Все функции для чтения данных из БД будут здесь

import sqlite3
import os
import logging

logger = logging.getLogger(__name__)

# Путь к файлу базы данных (в специальной папке data, которая не удаляется)
DB_PATH = 'data/calculator.db'

def get_connection():
    """Создает и возвращает соединение с базой данных"""
    # Убеждаемся, что папка data существует
    os.makedirs('data', exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Позволяет обращаться к колонкам по имени
    return conn

def init_database():
    """Создает таблицы, если их нет (вызывается при первом запуске)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Создаем таблицу номенклатуры
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS nomenclature (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                fixed_production REAL DEFAULT 0,
                output_per_drawing INTEGER DEFAULT 1,
                category TEXT
            )
        ''')
        
        # Создаем таблицу спецификаций
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS specifications (
                parent_code TEXT,
                child_code TEXT,
                quantity REAL,
                PRIMARY KEY (parent_code, child_code)
            )
        ''')
        
        conn.commit()
        logger.info("✅ Таблицы в БД созданы или уже существуют")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при создании таблиц: {e}")
    finally:
        conn.close()

def get_all_categories():
    """Возвращает список всех уникальных категорий"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT category FROM nomenclature WHERE category IS NOT NULL")
        rows = cursor.fetchall()
        return [row['category'] for row in rows]
    except Exception as e:
        logger.error(f"Ошибка получения категорий: {e}")
        return []
    finally:
        conn.close()

def get_products_by_category(category):
    """Возвращает список изделий/узлов по категории"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT code, name FROM nomenclature 
            WHERE type IN ('Изделие', 'Узел') 
            AND (category = ? OR ? = 'Все')
            ORDER BY name
        """, (category, category))
        rows = cursor.fetchall()
        return [{'code': row['code'], 'name': row['name']} for row in rows]
    except Exception as e:
        logger.error(f"Ошибка получения изделий: {e}")
        return []
    finally:
        conn.close()

def find_product_by_name(product_name):
    """Ищет изделие/узел по точному названию"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM nomenclature 
            WHERE name = ? AND type IN ('Изделие', 'Узел')
        """, (product_name,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None
    except Exception as e:
        logger.error(f"Ошибка поиска изделия: {e}")
        return None
    finally:
        conn.close()

def get_product_by_code(code):
    """Получает данные об изделии/узле/материале по коду"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM nomenclature WHERE code = ?", (code,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None
    except Exception as e:
        logger.error(f"Ошибка получения данных по коду: {e}")
        return None
    finally:
        conn.close()

def get_child_materials(parent_code):
    """Возвращает все прямые материалы для указанного родителя"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT child_code, quantity FROM specifications 
            WHERE parent_code = ?
        """, (parent_code,))
        rows = cursor.fetchall()
        return [{'child_code': row['child_code'], 'quantity': row['quantity']} for row in rows]
    except Exception as e:
        logger.error(f"Ошибка получения материалов: {e}")
        return []
    finally:
        conn.close()

def collect_all_materials(parent_code, multiplier=1):
    """Рекурсивно собирает все материалы для указанного изделия/узла"""
    materials = {}
    
    def explode(code, mult):
        children = get_child_materials(code)
        for child in children:
            child_data = get_product_by_code(child['child_code'])
            if not child_data:
                continue
            
            if child_data['type'] == 'Материал':
                if child['child_code'] not in materials:
                    materials[child['child_code']] = {
                        'name': child_data['name'],
                        'baseQty': 0
                    }
                materials[child['child_code']]['baseQty'] += child['quantity'] * mult
            elif child_data['type'] == 'Узел':
                explode(child['child_code'], mult * child['quantity'])
    
    explode(parent_code, multiplier)
    return materials
