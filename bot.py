import logging
import time
import pandas as pd
import requests
import sqlite3
import os
from io import BytesIO
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from config import TOKEN, GROUP_ID, TOPIC_ID, YANDEX_TABLE_URL, CACHE_TTL

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== БАЗА ДАННЫХ ЦЕН ====================
PRICES_DB_PATH = 'data/prices.db'

def init_prices_db():
    """Создает таблицу для хранения цен, если её нет"""
    os.makedirs('data', exist_ok=True)
    conn = sqlite3.connect(PRICES_DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS material_prices (
            material_name TEXT PRIMARY KEY,
            price REAL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
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

def save_material_price(material_name, price):
    try:
        conn = sqlite3.connect(PRICES_DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO material_prices (material_name, price, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (material_name, price))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка сохранения цены материала: {e}")

def get_all_material_prices():
    try:
        conn = sqlite3.connect(PRICES_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT material_name, price FROM material_prices")
        rows = cursor.fetchall()
        conn.close()
        return {row[0]: row[1] for row in rows}
    except Exception as e:
        logger.error(f"Ошибка получения цен: {e}")
        return {}

def save_drawing_price(product_code, price):
    try:
        conn = sqlite3.connect(PRICES_DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO drawing_prices (product_code, price, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (product_code, price))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка сохранения цены чертежа: {e}")

def get_drawing_price(product_code):
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

# ==================== БЛОКИРОВКА ПОЛЬЗОВАТЕЛЕЙ ====================
class UserLock:
    def __init__(self):
        self.current_user = None
        self.lock_time = 0
        self.username = None
        self.first_name = None
    
    def acquire(self, user_id, username=None, first_name=None):
        if self.current_user is None or (time.time() - self.lock_time) > 600:
            self.current_user = user_id
            self.lock_time = time.time()
            self.username = username
            self.first_name = first_name
            return True
        return False
    
    def release(self, user_id):
        if self.current_user == user_id:
            self.current_user = None
            self.username = None
            self.first_name = None
            logger.info(f"🔓 Блокировка освобождена")
    
    def is_locked(self):
        return self.current_user is not None
    
    def get_lock_info(self):
        if self.current_user:
            return {
                'user_id': self.current_user,
                'username': self.username,
                'first_name': self.first_name
            }
        return None

bot_lock = UserLock()

# ==================== ХРАНИЛИЩЕ ДАННЫХ ====================
cached_data = None
last_update = 0
sessions = {}

# ==================== ЗАГРУЗКА С GOOGLE ДИСКА ====================
def load_from_yandex():
    global cached_data, last_update
    
    current_time = time.time()
    if cached_data and (current_time - last_update) < CACHE_TTL:
        logger.info("Используем кэшированные данные")
        return cached_data
    
    if not YANDEX_TABLE_URL:
        logger.error("YANDEX_TABLE_URL не задан")
        return {'nomenclature': [], 'specifications': []}
    
    try:
        logger.info(f"Загрузка файла: {YANDEX_TABLE_URL}")
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(YANDEX_TABLE_URL, headers=headers, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"Ошибка загрузки: HTTP {response.status_code}")
            return {'nomenclature': [], 'specifications': []}
        
        workbook = pd.ExcelFile(BytesIO(response.content))
        nomenclature = pd.read_excel(workbook, sheet_name='Номенклатура').to_dict('records')
        specifications = pd.read_excel(workbook, sheet_name='Спецификации').to_dict('records')
        
        cached_data = {
            'nomenclature': nomenclature,
            'specifications': specifications
        }
        last_update = current_time
        
        logger.info(f"Загружено: номенклатура {len(nomenclature)} записей, спецификации {len(specifications)} записей")
        return cached_data
        
    except Exception as e:
        logger.error(f"Ошибка загрузки: {e}")
        return {'nomenclature': [], 'specifications': []}

# ==================== ПРОВЕРКА ДОСТУПА ====================
async def check_access(update: Update) -> bool:
    if not GROUP_ID or not TOPIC_ID:
        return True
    
    chat_id = update.effective_chat.id
    topic_id = update.message.message_thread_id if update.message else None
    
    if chat_id == GROUP_ID and topic_id == TOPIC_ID:
        return True
    
    logger.warning(f"Доступ запрещен: chat={chat_id}, topic={topic_id}")
    return False

# ==================== СБОР МАТЕРИАЛОВ ====================
def collect_materials(product_code, multiplier, nomenclature, specifications):
    materials = {}
    
    def explode(code, mult):
        for spec in specifications:
            parent = spec.get('Родитель') or spec.get('parent')
            child = spec.get('Потомок') or spec.get('child')
            quantity = spec.get('Количество') or spec.get('quantity', 0)
            
            if parent and str(parent) == str(code):
                for item in nomenclature:
                    item_code = item.get('Код') or item.get('code')
                    if item_code and str(item_code) == str(child):
                        item_type = str(item.get('Тип') or '').lower()
                        item_name = item.get('Наименование') or item.get('name', 'Неизвестно')
                        
                        if 'материал' in item_type:
                            if child not in materials:
                                materials[child] = {
                                    'name': item_name,
                                    'baseQty': 0
                                }
                            materials[child]['baseQty'] += float(quantity) * mult
                        elif 'узел' in item_type:
                            explode(child, mult * float(quantity))
    
    explode(product_code, multiplier)
    return materials

# ==================== ФОРМАТИРОВАНИЕ ====================
def format_number(num):
    return f"{num:,.2f}".replace(",", " ")

def format_materials_for_input(materials_list, saved_prices):
    result = "📦 *Материалы*\n\nВведите цены через пробел в том же порядке:\n\n"
    for m in materials_list:
        saved = saved_prices.get(m['name'], 0)
        if saved > 0:
            result += f"{m['number']}. {m['name']} — нужно {format_number(m['qty'])} шт *(сохранённая цена: {format_number(saved)} ISK)*\n"
        else:
            result += f"{m['number']}. {m['name']} — нужно {format_number(m['qty'])} шт\n"
    return result

def format_results(product_name, qty, efficiency, tax_rate, materials_list, result):
    text = f"📊 *РЕЗУЛЬТАТЫ РАСЧЕТА*\n\n"
    text += f"Изделие: {product_name}\n"
    text += f"Количество: {qty:.0f} шт\n"
    text += f"Эффективность: {efficiency:.0f}%\n"
    text += f"Налог: {tax_rate:.0f}%\n\n"
    
    text += "*Материалы:*\n"
    for m in materials_list:
        text += f"{m['number']}. {m['name']}: {format_number(m['qty'])} шт × {format_number(m['price'])} ISK = {format_number(m['cost'])} ISK\n"
    
    text += f"\n💰 *ИТОГИ*\n"
    text += f"Материалы: {format_number(result['materialCost'])} ISK\n"
    text += f"Производство: {format_number(result['prodCost'])} ISK\n"
    text += f"Чертежи: {format_number(result['drawingCost'])} ISK\n"
    text += f"Себестоимость: {format_number(result['totalCost'])} ISK\n"
    text += f"Выручка: {format_number(result['revenue'])} ISK\n"
    text += f"Прибыль до налога: {format_number(result['profitBeforeTax'])} ISK\n"
    text += f"Налог: {format_number(result['tax'])} ISK\n"
    text += f"*Прибыль после налога: {format_number(result['profitAfterTax'])} ISK*\n"
    
    if qty > 0:
        per_unit = result['totalCost'] / qty
        per_unit_profit = result['profitAfterTax'] / qty
        text += f"\n*НА 1 ШТУКУ:*\n"
        text += f"Себестоимость: {format_number(per_unit)} ISK\n"
        text += f"Прибыль: {format_number(per_unit_profit)} ISK\n"
    
    return text

async def show_products_page(update_or_query, session, edit: bool = False):
    products = session['products']
    page = session.get('product_page', 0)
    items_per_page = 20
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(products))
    total_pages = (len(products) - 1) // items_per_page + 1
    
    text = f"📋 *Доступные изделия (страница {page + 1}/{total_pages}):*\n\n"
    for i in range(start_idx, end_idx):
        text += f"{i+1}. {products[i]['name']}\n"
    text += f"\n👉 Введите номер изделия (1-{len(products)}) для выбора"
    
    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Назад", callback_data="prev_page"))
    if end_idx < len(products):
        nav_row.append(InlineKeyboardButton("Вперед ▶️", callback_data="next_page"))
    if nav_row:
        keyboard.append(nav_row)
    
    keyboard.append([InlineKeyboardButton("🔙 К категориям", callback_data="back_to_categories")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    
    if edit:
        await update_or_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await update_or_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# ==================== ОБРАБОТЧИКИ КОМАНД ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update):
        return
    
    user_id = update.effective_user.id
    username = update.effective_user.username
    first_name = update.effective_user.first_name
    
    if bot_lock.is_locked() and bot_lock.current_user != user_id:
        lock_info = bot_lock.get_lock_info()
        name = lock_info['first_name'] or f"@{lock_info['username']}" if lock_info['username'] else f"ID {lock_info['user_id']}"
        await update.message.reply_text(f"⏳ *Бот занят*\n\nСейчас расчёты выполняет: *{name}*", parse_mode='Markdown')
        return
    
    if not bot_lock.acquire(user_id, username, first_name):
        await update.message.reply_text("❌ Не удалось начать расчет. Попробуйте позже.")
        return
    
    sessions.pop(user_id, None)
    sessions[user_id] = {'step': 'categories'}
    
    data = load_from_yandex()
    
    if not data['nomenclature']:
        await update.message.reply_text("❌ Ошибка загрузки данных")
        bot_lock.release(user_id)
        return
    
    categories = list(set(item.get('Категории', '') for item in data['nomenclature'] if item.get('Категории')))
    
    if not categories:
        await update.message.reply_text("❌ В базе нет категорий")
        bot_lock.release(user_id)
        return
    
    keyboard = []
    for cat in sorted(categories):
        keyboard.append([InlineKeyboardButton(cat, callback_data=f"cat_{cat}")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    
    await update.message.reply_text(
        "👋 *Производственный калькулятор*\n\nВыберите категорию:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    data = query.data
    
    if data != "cancel" and bot_lock.is_locked() and bot_lock.current_user != user_id:
        lock_info = bot_lock.get_lock_info()
        name = lock_info['first_name'] or f"@{lock_info['username']}" if lock_info['username'] else f"ID {lock_info['user_id']}"
        await query.edit_message_text(f"⏳ *Бот занят*\n\nСейчас расчёты выполняет: *{name}*", parse_mode='Markdown')
        return
    
    if data == "cancel":
        bot_lock.release(user_id)
        sessions.pop(user_id, None)
        await query.edit_message_text("❌ Расчет отменен")
        return
    
    if data in ["prev_page", "next_page"]:
        session = sessions.get(user_id)
        if session and session.get('step') == 'product_selection':
            session['product_page'] += 1 if data == "next_page" else -1
            await show_products_page(query, session, edit=True)
        return
    
    if data == "back_to_categories":
        sessions.pop(user_id, None)
        data = load_from_yandex()
        categories = list(set(item.get('Категории', '') for item in data['nomenclature'] if item.get('Категории')))
        keyboard = []
        for cat in sorted(categories):
            keyboard.append([InlineKeyboardButton(cat, callback_data=f"cat_{cat}")])
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        await query.edit_message_text(
            "👋 *Производственный калькулятор*\n\nВыберите категорию:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return
    
    if data == "back_to_products":
        session = sessions.get(user_id)
        if session and session.get('step') in ['prices', 'quantity', 'material_prices']:
            session['step'] = 'product_selection'
            session['product_page'] = 0
            await show_products_page(query, session, edit=True)
        return
    
    if data.startswith("cat_"):
        category = data[4:]
        sessions[user_id] = {'step': 'parameters', 'category': category}
        keyboard = [[InlineKeyboardButton("🔙 К категориям", callback_data="back_to_categories")]]
        await query.edit_message_text(
            f"📊 *Параметры расчета*\nКатегория: {category}\n\nВведите через пробел:\n`Эффективность (%) Налог (%)`\n\nПример: `150 20`",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update):
        return
    
    user_id = update.effective_user.id
    session = sessions.get(user_id)
    
    if not session:
        await update.message.reply_text("Используйте /start")
        return
    
    if bot_lock.is_locked() and bot_lock.current_user != user_id:
        lock_info = bot_lock.get_lock_info()
        name = lock_info['first_name'] or f"@{lock_info['username']}" if lock_info['username'] else f"ID {lock_info['user_id']}"
        await update.message.reply_text(f"⏳ *Бот занят*\n\nСейчас расчёты выполняет: *{name}*", parse_mode='Markdown')
        return
    
    text = update.message.text
    logger.info(f"Текст от {user_id}: {text}, шаг: {session.get('step')}")
    
    if session['step'] == 'parameters':
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text("❌ Введите через пробел: Эффективность Налог\nПример: 150 20")
            return
        
        try:
            efficiency, tax = float(parts[0]), float(parts[1])
        except ValueError:
            await update.message.reply_text("❌ Введите числа")
            return
        
        data = load_from_yandex()
        products = []
        for item in data['nomenclature']:
            if item.get('Категории') == session['category'] and 'изделие' in str(item.get('Тип', '')).lower():
                products.append({'code': item['Код'], 'name': item['Наименование']})
        
        if not products:
            await update.message.reply_text("❌ Нет изделий в этой категории")
            return
        
        session.update({'step': 'product_selection', 'efficiency': efficiency, 'tax': tax, 'products': products, 'product_page': 0})
        await show_products_page(update, session, edit=False)
        return
    
    elif session['step'] == 'product_selection':
        try:
            idx = int(text) - 1
            if idx < 0 or idx >= len(session['products']):
                raise ValueError
            selected = session['products'][idx]
        except:
            await update.message.reply_text(f"❌ Введите число от 1 до {len(session['products'])}")
            return
        
        data = load_from_yandex()
        product = next((item for item in data['nomenclature'] if item['Код'] == selected['code']), None)
        
        if not product:
            await update.message.reply_text("❌ Ошибка получения данных")
            return
        
        multiplicity = product.get('Кратность', 1)
        if multiplicity is None or multiplicity == '' or (isinstance(multiplicity, float) and pd.isna(multiplicity)):
            multiplicity = 1
        else:
            multiplicity = int(float(multiplicity))
        
        session.update({'step': 'prices', 'product': product, 'output_per_drawing': multiplicity})
        
        saved_price = get_drawing_price(product['Код'])
        price_text = f"✅ Выбрано: *{product['Наименование']}*\nКратность: {multiplicity}\n\n💰 Введите через пробел:\n`Рыночная цена Стоимость чертежа`\n\n"
        if saved_price > 0:
            price_text += f"*(сохранённая стоимость чертежа: {format_number(saved_price)} ISK)*\n"
        price_text += f"Пример: `3200000 6900000`"
        
        await update.message.reply_text(price_text, parse_mode='Markdown')
        return
    
    elif session['step'] == 'prices':
        parts = text.split()
        if len(parts) < 2:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data="back_to_products")]]
            await update.message.reply_text("❌ Введите две цены через пробел\nПример: 3200000 6900000", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        try:
            market_price, drawing_price = float(parts[0]), float(parts[1])
        except ValueError:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data="back_to_products")]]
            await update.message.reply_text("❌ Введите числа", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        save_d
# ==================== ОБРАБОТЧИКИ КОМАНД ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update):
        return
    
    user_id = update.effective_user.id
    username = update.effective_user.username
    first_name = update.effective_user.first_name
    
    if bot_lock.is_locked() and bot_lock.current_user != user_id:
        lock_info = bot_lock.get_lock_info()
        name = lock_info['first_name'] or f"@{lock_info['username']}" if lock_info['username'] else f"ID {lock_info['user_id']}"
        await update.message.reply_text(f"⏳ *Бот занят*\n\nСейчас расчёты выполняет: *{name}*", parse_mode='Markdown')
        return
    
    if not bot_lock.acquire(user_id, username, first_name):
        await update.message.reply_text("❌ Не удалось начать расчет. Попробуйте позже.")
        return
    
    sessions.pop(user_id, None)
    sessions[user_id] = {'step': 'categories'}
    
    data = load_from_yandex()
    
    if not data['nomenclature']:
        await update.message.reply_text("❌ Ошибка загрузки данных")
        bot_lock.release(user_id)
        return
    
    categories = list(set(item.get('Категории', '') for item in data['nomenclature'] if item.get('Категории')))
    
    if not categories:
        await update.message.reply_text("❌ В базе нет категорий")
        bot_lock.release(user_id)
        return
    
    keyboard = []
    for cat in sorted(categories):
        keyboard.append([InlineKeyboardButton(cat, callback_data=f"cat_{cat}")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    
    await update.message.reply_text(
        "👋 *Производственный калькулятор*\n\nВыберите категорию:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    data = query.data
    
    if data != "cancel" and bot_lock.is_locked() and bot_lock.current_user != user_id:
        lock_info = bot_lock.get_lock_info()
        name = lock_info['first_name'] or f"@{lock_info['username']}" if lock_info['username'] else f"ID {lock_info['user_id']}"
        await query.edit_message_text(f"⏳ *Бот занят*\n\nСейчас расчёты выполняет: *{name}*", parse_mode='Markdown')
        return
    
    if data == "cancel":
        bot_lock.release(user_id)
        sessions.pop(user_id, None)
        await query.edit_message_text("❌ Расчет отменен")
        return
    
    if data in ["prev_page", "next_page"]:
        session = sessions.get(user_id)
        if session and session.get('step') == 'product_selection':
            session['product_page'] += 1 if data == "next_page" else -1
            await show_products_page(query, session, edit=True)
        return
    
    if data == "back_to_categories":
        sessions.pop(user_id, None)
        data = load_from_yandex()
        categories = list(set(item.get('Категории', '') for item in data['nomenclature'] if item.get('Категории')))
        keyboard = []
        for cat in sorted(categories):
            keyboard.append([InlineKeyboardButton(cat, callback_data=f"cat_{cat}")])
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        await query.edit_message_text(
            "👋 *Производственный калькулятор*\n\nВыберите категорию:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return
    
    if data == "back_to_products":
        session = sessions.get(user_id)
        if session and session.get('step') in ['prices', 'quantity', 'material_prices']:
            session['step'] = 'product_selection'
            session['product_page'] = 0
            await show_products_page(query, session, edit=True)
        return
    
    if data.startswith("cat_"):
        category = data[4:]
        sessions[user_id] = {'step': 'parameters', 'category': category}
        keyboard = [[InlineKeyboardButton("🔙 К категориям", callback_data="back_to_categories")]]
        await query.edit_message_text(
            f"📊 *Параметры расчета*\nКатегория: {category}\n\nВведите через пробел:\n`Эффективность (%) Налог (%)`\n\nПример: `150 20`",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update):
        return
    
    user_id = update.effective_user.id
    session = sessions.get(user_id)
    
    if not session:
        await update.message.reply_text("Используйте /start")
        return
    
    if bot_lock.is_locked() and bot_lock.current_user != user_id:
        lock_info = bot_lock.get_lock_info()
        name = lock_info['first_name'] or f"@{lock_info['username']}" if lock_info['username'] else f"ID {lock_info['user_id']}"
        await update.message.reply_text(f"⏳ *Бот занят*\n\nСейчас расчёты выполняет: *{name}*", parse_mode='Markdown')
        return
    
    text = update.message.text
    logger.info(f"Текст от {user_id}: {text}, шаг: {session.get('step')}")
    
    if session['step'] == 'parameters':
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text("❌ Введите через пробел: Эффективность Налог\nПример: 150 20")
            return
        
        try:
            efficiency, tax = float(parts[0]), float(parts[1])
        except ValueError:
            await update.message.reply_text("❌ Введите числа")
            return
        
        data = load_from_yandex()
        products = []
        for item in data['nomenclature']:
            if item.get('Категории') == session['category'] and 'изделие' in str(item.get('Тип', '')).lower():
                products.append({'code': item['Код'], 'name': item['Наименование']})
        
        if not products:
            await update.message.reply_text("❌ Нет изделий в этой категории")
            return
        
        session.update({'step': 'product_selection', 'efficiency': efficiency, 'tax': tax, 'products': products, 'product_page': 0})
        await show_products_page(update, session, edit=False)
        return
    
    elif session['step'] == 'product_selection':
        try:
            idx = int(text) - 1
            if idx < 0 or idx >= len(session['products']):
                raise ValueError
            selected = session['products'][idx]
        except:
            await update.message.reply_text(f"❌ Введите число от 1 до {len(session['products'])}")
            return
        
        data = load_from_yandex()
        product = next((item for item in data['nomenclature'] if item['Код'] == selected['code']), None)
        
        if not product:
            await update.message.reply_text("❌ Ошибка получения данных")
            return
        
        multiplicity = product.get('Кратность', 1)
        if multiplicity is None or multiplicity == '' or (isinstance(multiplicity, float) and pd.isna(multiplicity)):
            multiplicity = 1
        else:
            multiplicity = int(float(multiplicity))
        
        session.update({'step': 'prices', 'product': product, 'output_per_drawing': multiplicity})
        
        saved_price = get_drawing_price(product['Код'])
        price_text = f"✅ Выбрано: *{product['Наименование']}*\nКратность: {multiplicity}\n\n💰 Введите через пробел:\n`Рыночная цена Стоимость чертежа`\n\n"
        if saved_price > 0:
            price_text += f"*(сохранённая стоимость чертежа: {format_number(saved_price)} ISK)*\n"
        price_text += f"Пример: `3200000 6900000`"
        
        await update.message.reply_text(price_text, parse_mode='Markdown')
        return
    
    elif session['step'] == 'prices':
        parts = text.split()
        if len(parts) < 2:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data="back_to_products")]]
            await update.message.reply_text("❌ Введите две цены через пробел\nПример: 3200000 6900000", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        try:
            market_price, drawing_price = float(parts[0]), float(parts[1])
        except ValueError:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data="back_to_products")]]
            await update.message.reply_text("❌ Введите числа", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        save_drawing_price(session['product']['Код'], drawing_price)
        session.update({'market_price': market_price, 'drawing_price': drawing_price, 'step': 'quantity'})
        await update.message.reply_text("📦 Введите количество продукции (шт):")
        return
    
    elif session['step'] == 'quantity':
        try:
            qty = float(text)
        except ValueError:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data="back_to_products")]]
            await update.message.reply_text("❌ Введите число", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        output = session['output_per_drawing']
        if qty % output != 0:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data="back_to_products")]]
            await update.message.reply_text(f"⚠️ Количество должно быть кратно {output}", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        drawings_needed = int(qty // output)
        data = load_from_yandex()
        materials_dict = collect_materials(session['product']['Код'], 1, data['nomenclature'], data['specifications'])
        
        if not materials_dict:
            await update.message.reply_text("❌ Нет материалов для этого изделия")
            return
        
        materials_list = []
        i = 1
        for m in materials_dict.values():
            raw = (m['baseQty'] / 1.5) * (session['efficiency'] / 100)
            rounded = (raw * 10 // 1 + 1) / 10 if raw * 10 % 1 > 0 else raw
            materials_list.append({'number': i, 'name': m['name'], 'qty': rounded * drawings_needed})
            i += 1
        
        saved_prices = get_all_material_prices()
        session.update({'step': 'material_prices', 'qty': qty, 'drawings_needed': drawings_needed, 'materials_list': materials_list})
        await update.message.reply_text(format_materials_for_input(materials_list, saved_prices), parse_mode='Markdown')
        return
    
    elif session['step'] == 'material_prices':
        parts = text.split()
        if len(parts) < len(session['materials_list']):
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data="back_to_products")]]
            await update.message.reply_text(f"❌ Введите {len(session['materials_list'])} цен через пробел", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        try:
            prices = [float(p) for p in parts[:len(session['materials_list'])]]
        except ValueError:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data="back_to_products")]]
            await update.message.reply_text("❌ Введите числа", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        for i, m in enumerate(session['materials_list']):
            m['price'] = prices[i]
            m['cost'] = m['qty'] * prices[i]
            save_material_price(m['name'], prices[i])
        
        material_cost = sum(m['qty'] * m['price'] for m in session['materials_list'])
        
        price_str = str(session['product'].get('Цена производства', '0')).replace(' ISK', '').replace(' ', '')
        try:
            prod_cost = float(price_str) * session['drawings_needed']
        except:
            prod_cost = 0
        
        total = material_cost + prod_cost + session['drawing_price'] * session['drawings_needed']
        revenue = session['market_price'] * session['qty']
        profit_before = revenue - total
        tax = profit_before * session['tax'] / 100 if profit_before > 0 else 0
        
        result = {
            'materialCost': material_cost,
            'prodCost': prod_cost,
            'drawingCost': session['drawing_price'] * session['drawings_needed'],
            'totalCost': total,
            'revenue': revenue,
            'profitBeforeTax': profit_before,
            'tax': tax,
            'profitAfterTax': profit_before - tax
        }
        
        await update.message.reply_text(
            format_results(session['product']['Наименование'], session['qty'], session['efficiency'], session['tax'], session['materials_list'], result),
            parse_mode='Markdown'
        )
        
        sessions.pop(user_id, None)
        bot_lock.release(user_id)
        return

# ==================== ЗАПУСК ====================
def main():
    init_prices_db()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    logger.info("✅ Бот запущен")
    app.run_polling()

if __name__ == "__main__":
    main()
