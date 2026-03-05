import sqlite3
import os

DB_PATH = 'data/calculator.db'

print(f"Проверяем файл: {DB_PATH}")
print(f"Файл существует: {os.path.exists(DB_PATH)}")

if not os.path.exists(DB_PATH):
    print("❌ Файл базы не найден!")
    exit()

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Получаем список всех таблиц
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(f"\n📊 Таблицы в базе: {tables}")

# Проверяем таблицу nomenclature
if tables:
    for table in tables:
        table_name = table[0]
        print(f"\n--- Таблица: {table_name} ---")
        
        # Получаем структуру таблицы
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        print("Колонки:")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
        
        # Получаем количество записей
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"Количество записей: {count}")
        
        # Показываем первые 3 записи
        if count > 0:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
            rows = cursor.fetchall()
            print("Первые 3 записи:")
            for row in rows:
                print(f"  {row}")

conn.close()
