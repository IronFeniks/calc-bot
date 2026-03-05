import sqlite3
import os

DB_PATH = 'data/calculator.db'

print("🔍 ПРОВЕРКА СТРУКТУРЫ БАЗЫ ДАННЫХ")
print("=" * 50)

# Проверяем существование файла
print(f"Файл: {DB_PATH}")
print(f"Существует: {os.path.exists(DB_PATH)}")

if not os.path.exists(DB_PATH):
    print("❌ Файл не найден!")
    exit()

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Получаем список всех таблиц
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(f"\n📊 Таблицы в базе: {[t[0] for t in tables]}")

# Для каждой таблицы показываем структуру
for table in tables:
    table_name = table[0]
    print(f"\n--- Таблица: {table_name} ---")
    
    # Получаем колонки
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    print("Колонки:")
    for col in columns:
        print(f"  • {col[1]} (тип: {col[2]})")
    
    # Получаем количество записей
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f"Количество записей: {count}")
    
    # Показываем первые 3 записи
    if count > 0:
        print("Первые 3 записи:")
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
        rows = cursor.fetchall()
        for row in rows:
            print(f"  {row}")
    
    # Проверяем, есть ли колонка category
    col_names = [col[1].lower() for col in columns]
    if 'category' in col_names:
        print("✅ Колонка 'category' найдена")
        
        # Проверяем, есть ли значения в category
        cursor.execute(f"SELECT DISTINCT category FROM {table_name} WHERE category IS NOT NULL AND category != '' LIMIT 10")
        cats = cursor.fetchall()
        if cats:
            print(f"✅ Найденные категории: {[c[0] for c in cats]}")
        else:
            print("❌ Категории есть, но все пустые")
    else:
        print("❌ Колонка 'category' НЕ найдена")

conn.close()
print("\n" + "=" * 50)
