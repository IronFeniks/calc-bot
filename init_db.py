# init_db.py - скрипт для первоначального заполнения базы данных
# Запустить один раз: python init_db.py

import sqlite3
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Путь к базе данных (такой же, как в database.py)
DB_PATH = 'data/calculator.db'

def init_database():
    """Создает таблицы и заполняет их тестовыми данными"""
    
    # Убеждаемся, что папка data существует
    os.makedirs('data', exist_ok=True)
    
    # Если база уже существует - удаляем её (чтобы пересоздать с новыми данными)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        logger.info("Старая база данных удалена")
    
    # Подключаемся (автоматически создаст новый файл)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    logger.info("Создание таблиц...")
    
    # Создаем таблицу номенклатуры
    cursor.execute('''
        CREATE TABLE nomenclature (
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
        CREATE TABLE specifications (
            parent_code TEXT,
            child_code TEXT,
            quantity REAL,
            PRIMARY KEY (parent_code, child_code)
        )
    ''')
    
    logger.info("Заполнение номенклатуры тестовыми данными...")
    
    # Вставляем тестовые данные в номенклатуру
    nomenclature_data = [
        # Изделия и узлы
        ('Изд001', 'Балка', 'Изделие', 500000, 10, 'Сооружения'),
        ('Изд002', 'Каркас', 'Узел', 200000, 5, 'Сооружения'),
        ('Изд003', 'Крепление', 'Узел', 100000, 10, 'Сооружения'),
        
        # Материалы
        ('Мат001', 'Болт М10', 'Материал', 0, 1, 'Такелаж'),
        ('Мат002', 'Гайка М10', 'Материал', 0, 1, 'Такелаж'),
        ('Мат003', 'Шайба М10', 'Материал', 0, 1, 'Такелаж'),
        ('Мат004', 'Краска', 'Материал', 0, 1, 'Расходники'),
    ]
    
    cursor.executemany(
        "INSERT INTO nomenclature VALUES (?,?,?,?,?,?)",
        nomenclature_data
    )
    
    logger.info(f"Добавлено {len(nomenclature_data)} записей в номенклатуру")
    
    logger.info("Заполнение спецификаций...")
    
    # Вставляем связи между изделиями и материалами
    specifications_data = [
        # Балка (Изд001) состоит из Каркаса (Изд002) и материалов
        ('Изд001', 'Изд002', 1.0),
        ('Изд001', 'Мат001', 4.0),
        ('Изд001', 'Мат002', 4.0),
        ('Изд001', 'Мат003', 4.0),
        
        # Каркас (Изд002) состоит из материалов
        ('Изд002', 'Мат001', 2.0),
        ('Изд002', 'Мат002', 2.0),
        ('Изд002', 'Мат003', 2.0),
        
        # Крепление (Изд003) состоит из материалов
        ('Изд003', 'Мат001', 1.0),
        ('Изд003', 'Мат002', 1.0),
    ]
    
    cursor.executemany(
        "INSERT INTO specifications VALUES (?,?,?)",
        specifications_data
    )
    
    logger.info(f"Добавлено {len(specifications_data)} связей в спецификации")
    
    # Сохраняем изменения
    conn.commit()
    
    # Проверяем, что данные добавились
    cursor.execute("SELECT COUNT(*) FROM nomenclature")
    count_nom = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM specifications")
    count_spec = cursor.fetchone()[0]
    
    logger.info(f"✅ База данных успешно создана!")
    logger.info(f"   Номенклатура: {count_nom} записей")
    logger.info(f"   Спецификации: {count_spec} записей")
    
    # Закрываем соединение
    conn.close()
    
    # Показываем где лежит база
    logger.info(f"📁 Файл базы данных: {os.path.abspath(DB_PATH)}")

if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("ЗАПУСК ИНИЦИАЛИЗАЦИИ БАЗЫ ДАННЫХ")
    init_database()
    logger.info("=" * 50)
