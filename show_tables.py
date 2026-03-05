import sqlite3
import os

DB_PATH = 'data/calculator.db'

print("🔍 ПРОВЕРКА БАЗЫ ДАННЫХ")
print("=" * 50)

# Проверяем существование файла
print(f"Путь к базе: {DB_PATH}")
print(f"Файл существует: {os.path.exists(DB_PATH)}")

if not os.path.exists(DB_PATH):
    print("❌ Файл базы не найден!")
    exit()

# Подключаемся
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Получаем все таблицы
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(f"\n📊 Найденные таблицы: {[t[0] for t in tables]}")

if not tables:
    print("❌ В базе нет таблиц!")
    exit()

# Для каждой таблицы показываем данные
for table in tables:
    table_name = table[0]
    print(f"\n--- Таблица: {table_name} ---")
    
    # Получаем структуру
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    col_names = [col[1] for col in columns]
    print(f"Колонки: {col_names}")
    
    # Получаем количество записей
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f"Записей: {count}")
    
    # Если есть записи, показываем первые 5
    if count > 0:
        print("Первые 5 записей:")
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
        rows = cursor.fetchall()
        for row in rows:
            print(f"  {row}")
    
    # Проверяем наличие категорий
    if 'category' in col_names:
        cursor.execute(f"SELECT DISTINCT category FROM {table_name} WHERE category IS NOT NULL AND category != ''")
        cats = cursor.fetchall()
        if cats:
            print(f"✅ Категории: {[c[0] for c in cats]}")
        else:
            print("❌ Категории не найдены (пустые значения)")

conn.close()
print("\n" + "=" * 50)
