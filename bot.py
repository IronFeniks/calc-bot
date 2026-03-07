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

# ==================== ФУНКЦИЯ ДЛЯ УСТАНОВКИ МЕНЮ ====================
async def post_init(application: Application):
    """Устанавливает меню с командами после запуска бота"""
    try:
        await application.bot.set_my_commands([
            ("start", "Начать новый расчет"),
            ("nalog", "Установка налога и эффективности"),
            ("categories", "Полный расчет по категориям"),
            ("automatic", "Автоматический расчет"),
            ("instructions", "Инструкция")
        ])
        logger.info("✅ Меню с командами установлено")
    except Exception as e:
        logger.error(f"❌ Ошибка установки меню: {e}")

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

# ==================== ФУНКЦИИ ДЛЯ АВТОМАТИЧЕСКОГО РЕЖИМА ====================

async def show_products_page(update_or_query, session, edit: bool = False):
    products = session['products']
    page = session.get('product_page', 0)
    items_per_page = 20
    user_id = session.get('user_id')
    
    if not user_id and hasattr(update_or_query, 'from_user'):
        user_id = update_or_query.from_user.id
    
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
        nav_row.append(InlineKeyboardButton("◀️ Назад", callback_data=f"user_{user_id}_prev_page"))
    if end_idx < len(products):
        nav_row.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"user_{user_id}_next_page"))
    if nav_row:
        keyboard.append(nav_row)
    
    keyboard.append([InlineKeyboardButton("🔙 К категориям", callback_data=f"user_{user_id}_back_to_categories")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    
    if edit:
        await update_or_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await update_or_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def show_automatic_products_page(update_or_query, session, edit: bool = False):
    """Показывает страницу со списком изделий для автоматического расчета"""
    products = session['products']
    page = session.get('product_page', 0)
    items_per_page = 20
    user_id = session.get('user_id')
    
    if not user_id and hasattr(update_or_query, 'from_user'):
        user_id = update_or_query.from_user.id
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(products))
    total_pages = (len(products) - 1) // items_per_page + 1
    
    text = f"🤖 *Автоматический расчет (страница {page + 1}/{total_pages}):*\n\n"
    for i in range(start_idx, end_idx):
        text += f"{i+1}. {products[i]['name']}\n"
    text += f"\n👉 Введите номер изделия (1-{len(products)})"
    
    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Назад", callback_data=f"user_{user_id}_autoprev_page"))
    if end_idx < len(products):
        nav_row.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"user_{user_id}_autonext_page"))
    if nav_row:
        keyboard.append(nav_row)
    
    keyboard.append([InlineKeyboardButton("🔙 К категориям", callback_data=f"user_{user_id}_back_to_automatic_categories")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    
    if edit:
        await update_or_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await update_or_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def calculate_and_send_result(update, session, materials_list, qty, drawings_needed):
    """Расчет и отправка результата"""
    material_cost = sum(m['qty'] * m['price'] for m in materials_list)
    
    price_str = str(session['product'].get('Цена производства', '0')).replace(' ISK', '').replace(' ', '')
    try:
        prod_cost = float(price_str) if price_str else 0
    except:
        prod_cost = 0
    prod_cost = prod_cost * drawings_needed
    
    drawing_cost = session['drawing_price'] * drawings_needed
    total = material_cost + prod_cost + drawing_cost
    revenue = session['market_price'] * session['qty']
    profit_before = revenue - total
    tax = profit_before * session['tax'] / 100 if profit_before > 0 else 0
    profit_after = profit_before - tax
    
    result = {
        'materialCost': material_cost,
        'prodCost': prod_cost,
        'drawingCost': drawing_cost,
        'totalCost': total,
        'revenue': revenue,
        'profitBeforeTax': profit_before,
        'tax': tax,
        'profitAfterTax': profit_after
    }
    
    await update.message.reply_text(
        format_results(
            session['product']['Наименование'],
            session['qty'],
            session['efficiency'],
            session['tax'],
            materials_list,
            result
        ),
        parse_mode='Markdown'
    )
    
    sessions.pop(update.effective_user.id, None)
    bot_lock.release(update.effective_user.id)

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
    sessions[user_id] = {'step': 'categories', 'user_id': user_id}
    
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
    
    categories_str = [str(cat) for cat in categories]
    
    keyboard = []
    for cat in sorted(categories_str):
        callback_data = f"user_{user_id}_cat_{cat}"
        keyboard.append([InlineKeyboardButton(cat, callback_data=callback_data)])
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    
    await update.message.reply_text(
        "👋 *Производственный калькулятор*\n\nВыберите категорию:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

# ==================== НОВЫЕ КОМАНДЫ МЕНЮ ====================

async def nalog_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /nalog - установка эффективности и налога"""
    user_id = update.effective_user.id
    
    if not await check_access(update):
        return
    
    if bot_lock.is_locked() and bot_lock.current_user != user_id:
        lock_info = bot_lock.get_lock_info()
        name = lock_info['first_name'] or f"@{lock_info['username']}" if lock_info['username'] else f"ID {lock_info['user_id']}"
        await update.message.reply_text(f"⏳ *Бот занят*\n\nСейчас расчёты выполняет: *{name}*", parse_mode='Markdown')
        return
    
    if not bot_lock.acquire(user_id, update.effective_user.username, update.effective_user.first_name):
        await update.message.reply_text("❌ Не удалось начать расчет. Попробуйте позже.")
        return
    
    if user_id not in sessions:
        sessions[user_id] = {'user_id': user_id}
    
    sessions[user_id]['step'] = 'awaiting_nalog'
    
    await update.message.reply_text(
        "📊 *Установка общих значений*\n\n"
        "Введите через пробел:\n"
        "`Эффективность (%) Налог (%)`\n\n"
        "Пример: `150 20`",
        parse_mode='Markdown'
    )

async def categories_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /categories - выбор категорий и полный расчет"""
    user_id = update.effective_user.id
    
    if not await check_access(update):
        return
    
    if bot_lock.is_locked() and bot_lock.current_user != user_id:
        lock_info = bot_lock.get_lock_info()
        name = lock_info['first_name'] or f"@{lock_info['username']}" if lock_info['username'] else f"ID {lock_info['user_id']}"
        await update.message.reply_text(f"⏳ *Бот занят*\n\nСейчас расчёты выполняет: *{name}*", parse_mode='Markdown')
        return
    
    if not bot_lock.acquire(user_id, update.effective_user.username, update.effective_user.first_name):
        await update.message.reply_text("❌ Не удалось начать расчет. Попробуйте позже.")
        return
    
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
    
    # Проверяем, есть ли уже сохраненные глобальные значения
    session = sessions.get(user_id, {})
    if 'global_efficiency' in session and 'global_tax' in session:
        # Если есть - сразу показываем категории
        efficiency = session['global_efficiency']
        tax = session['global_tax']
        
        categories_str = [str(cat) for cat in categories]
        keyboard = []
        for cat in sorted(categories_str):
            callback_data = f"user_{user_id}_cat_{cat}"
            keyboard.append([InlineKeyboardButton(cat, callback_data=callback_data)])
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        
        session.update({
            'step': 'categories_selection',
            'efficiency': efficiency,
            'tax': tax,
            'user_id': user_id
        })
        
        await update.message.reply_text(
            "📋 Выберите категорию:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        # Если нет - запрашиваем
        sessions[user_id] = {'step': 'awaiting_nalog_for_categories', 'user_id': user_id}
        await update.message.reply_text(
            "📊 *Полный расчет*\n\n"
            "Сначала введите общие значения:\n"
            "`Эффективность (%) Налог (%)`\n\n"
            "Пример: `150 20`",
            parse_mode='Markdown'
        )

async def automatic_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /automatic - автоматический расчет по сохраненным ценам"""
    user_id = update.effective_user.id
    
    if not await check_access(update):
        return
    
    if bot_lock.is_locked() and bot_lock.current_user != user_id:
        lock_info = bot_lock.get_lock_info()
        name = lock_info['first_name'] or f"@{lock_info['username']}" if lock_info['username'] else f"ID {lock_info['user_id']}"
        await update.message.reply_text(f"⏳ *Бот занят*\n\nСейчас расчёты выполняет: *{name}*", parse_mode='Markdown')
        return
    
    if not bot_lock.acquire(user_id, update.effective_user.username, update.effective_user.first_name):
        await update.message.reply_text("❌ Не удалось начать расчет. Попробуйте позже.")
        return
    
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
    
    # Проверяем, есть ли уже сохраненные глобальные значения
    session = sessions.get(user_id, {})
    if 'global_efficiency' in session and 'global_tax' in session:
        # Если есть - сразу показываем категории
        efficiency = session['global_efficiency']
        tax = session['global_tax']
        
        categories_str = [str(cat) for cat in categories]
        keyboard = []
        for cat in sorted(categories_str):
            callback_data = f"user_{user_id}_autocat_{cat}"
            keyboard.append([InlineKeyboardButton(cat, callback_data=callback_data)])
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        
        session.update({
            'step': 'automatic_categories',
            'efficiency': efficiency,
            'tax': tax,
            'automatic_mode': True,
            'user_id': user_id
        })
        
        await update.message.reply_text(
            "🤖 *Автоматический расчет*\n\nВыберите категорию:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        # Если нет - запрашиваем
        sessions[user_id] = {
            'step': 'awaiting_nalog_for_automatic',
            'automatic_mode': True,
            'user_id': user_id
        }
        await update.message.reply_text(
            "🤖 *Автоматический расчет*\n\n"
            "Сначала введите общие значения:\n"
            "`Эффективность (%) Налог (%)`\n\n"
            "Пример: `150 20`",
            parse_mode='Markdown'
        )

async def instructions_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /instructions - инструкция к калькулятору"""
    if not await check_access(update):
        return
    
    instruction_text = """
📚 *ИНСТРУКЦИЯ ПО КАЛЬКУЛЯТОРУ*

1️⃣ *Начало работы*
• Введите /start для нового расчета
• Используйте меню для быстрого доступа к командам

2️⃣ *Основные команды*
• /nalog - установить эффективность и налог
• /categories - полный расчет с выбором категории
• /automatic - расчет по сохраненным ценам
• /instructions - эта инструкция

3️⃣ *Сохранение параметров*
• Эффективность и налог сохраняются на время сессии
• При смене изделия в рамках одной категории значения не меняются
• При смене категории нужно ввести заново

4️⃣ *Процесс расчета*
• Выберите категорию изделия
• Введите эффективность и налог (один раз за сессию)
• Выберите изделие по номеру
• Введите рыночную цену и стоимость чертежа
• Введите количество продукции
• При необходимости введите цены материалов

5️⃣ *Сохранение цен*
• Цены материалов сохраняются автоматически
• При повторном расчете они подставляются

6️⃣ *Блокировка*
• Ботом может пользоваться только один человек
• Если бот занят, вы увидите, кто именно

❓ *Вопросы и предложения*
Обращайтесь к администратору
"""
    
    await update.message.reply_text(instruction_text, parse_mode='Markdown')

# ==================== ОБРАБОТЧИК КНОПОК ====================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    data = query.data
    
    # Проверяем, что callback data содержит ID пользователя
    if not data.startswith(f"user_{user_id}_") and data not in ["cancel"]:
        # Если это не кнопка отмены и не принадлежит текущему пользователю - игнорируем
        await query.answer("⛔ Эта кнопка не для вас", show_alert=True)
        return
    
    # Очищаем data от префикса с user_id
    clean_data = data
    if data.startswith(f"user_{user_id}_"):
        clean_data = data.replace(f"user_{user_id}_", "")
    
    # Проверяем блокировку (кроме кнопки отмены)
    if clean_data != "cancel" and bot_lock.is_locked() and bot_lock.current_user != user_id:
        lock_info = bot_lock.get_lock_info()
        name = lock_info['first_name'] or f"@{lock_info['username']}" if lock_info['username'] else f"ID {lock_info['user_id']}"
        await query.edit_message_text(f"⏳ *Бот занят*\n\nСейчас расчёты выполняет: *{name}*", parse_mode='Markdown')
        return
    
    if clean_data == "cancel":
        # Проверяем, что отменяет именно тот, кто начал расчет
        if bot_lock.current_user == user_id:
            bot_lock.release(user_id)
            sessions.pop(user_id, None)
            await query.edit_message_text("❌ Расчет отменен")
        else:
            await query.answer("⛔ Вы не можете отменить чужой расчет", show_alert=True)
        return
    
    if clean_data in ["prev_page", "next_page"]:
        session = sessions.get(user_id)
        if session and session.get('step') == 'product_selection':
            if clean_data == "prev_page":
                session['product_page'] = max(0, session.get('product_page', 0) - 1)
            else:
                session['product_page'] = session.get('product_page', 0) + 1
            await show_products_page(query, session, edit=True)
        return
    
    # Навигация для автоматического режима
    if clean_data in ["autoprev_page", "autonext_page"]:
        session = sessions.get(user_id)
        if session and session.get('step') == 'automatic_product_selection':
            if clean_data == "autoprev_page":
                session['product_page'] = max(0, session.get('product_page', 0) - 1)
            else:
                session['product_page'] = session.get('product_page', 0) + 1
            await show_automatic_products_page(query, session, edit=True)
        return
    
    if clean_data == "back_to_categories":
        # При возврате к категориям удаляем глобальные значения
        session = sessions.get(user_id, {})
        if 'global_efficiency' in session:
            del session['global_efficiency']
        if 'global_tax' in session:
            del session['global_tax']
        
        sessions.pop(user_id, None)
        data = load_from_yandex()
        categories = list(set(item.get('Категории', '') for item in data['nomenclature'] if item.get('Категории')))
        categories_str = [str(cat) for cat in categories]
        
        keyboard = []
        for cat in sorted(categories_str):
            callback_data = f"user_{user_id}_cat_{cat}"
            keyboard.append([InlineKeyboardButton(cat, callback_data=callback_data)])
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        
        await query.edit_message_text(
            "👋 *Производственный калькулятор*\n\nВыберите категорию:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return
    
    if clean_data == "back_to_automatic_categories":
        session = sessions.get(user_id)
        if session:
            # При возврате к категориям удаляем глобальные значения
            if 'global_efficiency' in session:
                del session['global_efficiency']
            if 'global_tax' in session:
                del session['global_tax']
            
            session['step'] = 'automatic_categories'
            data = load_from_yandex()
            categories = list(set(item.get('Категории', '') for item in data['nomenclature'] if item.get('Категории')))
            categories_str = [str(cat) for cat in categories]
            keyboard = []
            for cat in sorted(categories_str):
                callback_data = f"user_{user_id}_autocat_{cat}"
                keyboard.append([InlineKeyboardButton(cat, callback_data=callback_data)])
            keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
            await query.edit_message_text(
                "🤖 *Автоматический расчет*\n\nВыберите категорию:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        return
    
    if clean_data == "back_to_products":
        session = sessions.get(user_id)
        if session and session.get('step') in ['prices', 'quantity', 'material_prices', 'automatic_quantity', 'automatic_missing_prices']:
            session['step'] = 'product_selection'
            session['product_page'] = 0
            await show_products_page(query, session, edit=True)
        return
    
    if clean_data.startswith("cat_"):
        category = clean_data[4:]
        # При смене категории удаляем глобальные значения налога и эффективности
        session = sessions.get(user_id, {})
        if 'global_efficiency' in session:
            del session['global_efficiency']
        if 'global_tax' in session:
            del session['global_tax']
        
        sessions[user_id] = {'step': 'parameters', 'category': category, 'user_id': user_id}
        keyboard = [[InlineKeyboardButton("🔙 К категориям", callback_data=f"user_{user_id}_back_to_categories")]]
        await query.edit_message_text(
            f"📊 *Параметры расчета*\nКатегория: {category}\n\nВведите через пробел:\n`Эффективность (%) Налог (%)`\n\nПример: `150 20`",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return
    
    if clean_data.startswith("autocat_"):
        category = clean_data[8:]
        session = sessions.get(user_id)
        if not session:
            return
        
        # При смене категории удаляем глобальные значения
        if 'global_efficiency' in session:
            del session['global_efficiency']
        if 'global_tax' in session:
            del session['global_tax']
        
        session['category'] = category
        session['step'] = 'automatic_product_selection'
        
        data = load_from_yandex()
        products = []
        for item in data['nomenclature']:
            if item.get('Категории') == category and ('изделие' in str(item.get('Тип', '')).lower() or 'узел' in str(item.get('Тип', '')).lower()):
                products.append({'code': item['Код'], 'name': item['Наименование']})
        
        if not products:
            await query.edit_message_text("❌ Нет изделий в этой категории")
            return
        
        session['products'] = products
        session['product_page'] = 0
        await show_automatic_products_page(query, session, edit=True)
        return

# ==================== ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ ====================
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
    
    # Обработка ожидания ввода налога из разных источников
    if session.get('step') in ['awaiting_nalog', 'awaiting_nalog_for_categories', 'awaiting_nalog_for_automatic']:
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text("❌ Введите через пробел: Эффективность Налог\nПример: 150 20")
            return
        
        try:
            efficiency = float(parts[0])
            tax = float(parts[1])
        except ValueError:
            await update.message.reply_text("❌ Введите числа")
            return
        
        if session['step'] == 'awaiting_nalog':
            await update.message.reply_text(f"✅ Значения сохранены:\nЭффективность: {efficiency}%\nНалог: {tax}%")
            # Сохраняем глобальные значения в сессии
            session['global_efficiency'] = efficiency
            session['global_tax'] = tax
            sessions.pop(user_id, None)
            bot_lock.release(user_id)
            
        elif session['step'] == 'awaiting_nalog_for_categories':
            session.update({
                'step': 'categories_selection',
                'efficiency': efficiency,
                'tax': tax
            })
            
            data = load_from_yandex()
            categories = list(set(item.get('Категории', '') for item in data['nomenclature'] if item.get('Категории')))
            categories_str = [str(cat) for cat in categories]
            
            keyboard = []
            for cat in sorted(categories_str):
                callback_data = f"user_{user_id}_cat_{cat}"
                keyboard.append([InlineKeyboardButton(cat, callback_data=callback_data)])
            keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
            
            await update.message.reply_text(
                "📋 Выберите категорию:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        elif session['step'] == 'awaiting_nalog_for_automatic':
            session.update({
                'step': 'automatic_categories',
                'efficiency': efficiency,
                'tax': tax
            })
            
            data = load_from_yandex()
            categories = list(set(item.get('Категории', '') for item in data['nomenclature'] if item.get('Категории')))
            categories_str = [str(cat) for cat in categories]
            
            keyboard = []
            for cat in sorted(categories_str):
                callback_data = f"user_{user_id}_autocat_{cat}"
                keyboard.append([InlineKeyboardButton(cat, callback_data=callback_data)])
            keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
            
            await update.message.reply_text(
                "🤖 *Автоматический расчет*\n\nВыберите категорию:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        return
    
    # Шаг 1: Ввод параметров
    if session['step'] == 'parameters':
        # Проверяем, есть ли уже сохраненные глобальные значения
        if 'global_efficiency' in session and 'global_tax' in session:
            efficiency = session['global_efficiency']
            tax = session['global_tax']
            
            data = load_from_yandex()
            products = []
            for item in data['nomenclature']:
                category = item.get('Категории')
                item_type = str(item.get('Тип') or '').lower()
                if category == session['category'] and ('изделие' in item_type or 'узел' in item_type):
                    products.append({'code': item['Код'], 'name': item['Наименование']})
            
            if not products:
                await update.message.reply_text("❌ Нет изделий в этой категории")
                return
            
            session.update({
                'step': 'product_selection',
                'efficiency': efficiency,
                'tax': tax,
                'products': products,
                'product_page': 0
            })
            await show_products_page(update, session, edit=False)
            return
        else:
            parts = text.split()
            if len(parts) < 2:
                await update.message.reply_text("❌ Введите через пробел: Эффективность Налог\nПример: 150 20")
                return
            
            try:
                efficiency = float(parts[0])
                tax = float(parts[1])
            except ValueError:
                await update.message.reply_text("❌ Введите числа")
                return
            
            data = load_from_yandex()
            products = []
            for item in data['nomenclature']:
                category = item.get('Категории')
                item_type = str(item.get('Тип') or '').lower()
                if category == session['category'] and ('изделие' in item_type or 'узел' in item_type):
                    products.append({'code': item['Код'], 'name': item['Наименование']})
            
            if not products:
                await update.message.reply_text("❌ Нет изделий в этой категории")
                return
            
            session.update({
                'step': 'product_selection',
                'efficiency': efficiency,
                'tax': tax,
                'products': products,
                'product_page': 0
            })
            await show_products_page(update, session, edit=False)
            return
    
    # Шаг 2: Выбор изделия (для обоих режимов)
    elif session['step'] == 'product_selection' or session['step'] == 'automatic_product_selection':
        try:
            idx = int(text) - 1
            if idx < 0 or idx >= len(session['products']):
                raise ValueError
            selected = session['products'][idx]
        except:
            await update.message.reply_text(f"❌ Введите число от 1 до {len(session['products'])}")
            return
        
        is_automatic = session.get('step') == 'automatic_product_selection'
        
        # Получаем полные данные изделия из номенклатуры
        data = load_from_yandex()
        product = None
        for item in data['nomenclature']:
            if item['Код'] == selected['code']:
                product = item
                break
        
        if not product:
            await update.message.reply_text("❌ Ошибка получения данных")
            return
        
        # Получаем кратность с проверкой
        try:
            multiplicity = product.get('Кратность', 1)
            if multiplicity is None or multiplicity == '' or (isinstance(multiplicity, float) and pd.isna(multiplicity)):
                multiplicity = 1
            else:
                multiplicity = int(float(multiplicity))
        except:
            multiplicity = 1
        
        # Сохраняем выбранное изделие и переходим к следующему шагу
        if is_automatic:
            session.update({
                'step': 'automatic_quantity',
                'product': product,
                'output_per_drawing': multiplicity
            })
        else:
            session.update({
                'step': 'prices',
                'product': product,
                'output_per_drawing': multiplicity
            })
        
        saved_price = get_drawing_price(product['Код'])
        price_text = f"✅ Выбрано: *{product['Наименование']}*\nКратность: {multiplicity}\n\n"
        price_text += f"💰 Введите через пробел:\n`Рыночная цена Стоимость чертежа`\n\n"
        if saved_price > 0:
            price_text += f"*(сохранённая стоимость чертежа: {format_number(saved_price)} ISK)*\n"
        price_text += f"Пример: `3200000 6900000`"
        
        await update.message.reply_text(price_text, parse_mode='Markdown')
        return
    
    # Шаг 3: Ввод цен (обычный режим)
    elif session['step'] == 'prices':
        parts = text.split()
        if len(parts) < 2:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data=f"user_{user_id}_back_to_products")]]
            await update.message.reply_text("❌ Введите две цены через пробел\nПример: 3200000 6900000", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        try:
            market_price = float(parts[0])
            drawing_price = float(parts[1])
        except ValueError:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data=f"user_{user_id}_back_to_products")]]
            await update.message.reply_text("❌ Введите числа", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        save_drawing_price(session['product']['Код'], drawing_price)
        session.update({
            'market_price': market_price,
            'drawing_price': drawing_price,
            'step': 'quantity'
        })
        await update.message.reply_text("📦 Введите количество продукции (шт):")
        return
    
    # Шаг 4: Ввод количества (обычный режим)
    elif session['step'] == 'quantity':
        try:
            qty = float(text)
        except ValueError:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data=f"user_{user_id}_back_to_products")]]
            await update.message.reply_text("❌ Введите число", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        product = session['product']
        output = session['output_per_drawing']
        
        if qty % output != 0:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data=f"user_{user_id}_back_to_products")]]
            await update.message.reply_text(f"⚠️ Количество должно быть кратно {output}", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        drawings_needed = int(qty // output)
        data = load_from_yandex()
        
        materials_dict = collect_materials(
            product['Код'], 1,
            data['nomenclature'],
            data['specifications']
        )
        
        if not materials_dict:
            await update.message.reply_text("❌ Нет материалов для этого изделия")
            return
        
        materials_list = []
        i = 1
        for m in materials_dict.values():
            raw = (m['baseQty'] / 1.5) * (session['efficiency'] / 100)
            rounded = (raw * 10 // 1 + 1) / 10 if raw * 10 % 1 > 0 else raw
            final_qty = rounded * drawings_needed
            materials_list.append({
                'number': i,
                'name': m['name'],
                'qty': final_qty
            })
            i += 1
        
        saved_prices = get_all_material_prices()
        session.update({
            'step': 'material_prices',
            'qty': qty,
            'drawings_needed': drawings_needed,
            'materials_list': materials_list
        })
        
        await update.message.reply_text(
            format_materials_for_input(materials_list, saved_prices),
            parse_mode='Markdown'
        )
        return
    
    # Шаг 5: Ввод цен на материалы (обычный режим)
    elif session['step'] == 'material_prices':
        parts = text.split()
        if len(parts) < len(session['materials_list']):
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data=f"user_{user_id}_back_to_products")]]
            await update.message.reply_text(f"❌ Введите {len(session['materials_list'])} цен через пробел", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        try:
            prices = [float(p) for p in parts[:len(session['materials_list'])]]
        except ValueError:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data=f"user_{user_id}_back_to_products")]]
            await update.message.reply_text("❌ Введите числа", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        for i, m in enumerate(session['materials_list']):
            m['price'] = prices[i]
            m['cost'] = m['qty'] * prices[i]
            save_material_price(m['name'], prices[i])
        
        material_cost = sum(m['qty'] * m['price'] for m in session['materials_list'])
        
        price_str = str(session['product'].get('Цена производства', '0')).replace(' ISK', '').replace(' ', '')
        try:
            prod_cost = float(price_str) if price_str else 0
        except:
            prod_cost = 0
        prod_cost = prod_cost * session['drawings_needed']
        
        drawing_cost = session['drawing_price'] * session['drawings_needed']
        total = material_cost + prod_cost + drawing_cost
        revenue = session['market_price'] * session['qty']
        profit_before = revenue - total
        tax = profit_before * session['tax'] / 100 if profit_before > 0 else 0
        profit_after = profit_before - tax
        
        result = {
            'materialCost': material_cost,
            'prodCost': prod_cost,
            'drawingCost': drawing_cost,
            'totalCost': total,
            'revenue': revenue,
            'profitBeforeTax': profit_before,
            'tax': tax,
            'profitAfterTax': profit_after
        }
        
        await update.message.reply_text(
            format_results(
                session['product']['Наименование'],
                session['qty'],
                session['efficiency'],
                session['tax'],
                session['materials_list'],
                result
            ),
            parse_mode='Markdown'
        )
        
        sessions.pop(user_id, None)
        bot_lock.release(user_id)
        return
    
    # Шаг 4 (автоматический режим): Ввод количества
    elif session['step'] == 'automatic_quantity':
        try:
            qty = float(text)
        except ValueError:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data=f"user_{user_id}_back_to_products")]]
            await update.message.reply_text("❌ Введите число", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        product = session['product']
        output = session['output_per_drawing']
        
        if qty % output != 0:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data=f"user_{user_id}_back_to_products")]]
            await update.message.reply_text(f"⚠️ Количество должно быть кратно {output}", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        drawings_needed = int(qty // output)
        data = load_from_yandex()
        
        materials_dict = collect_materials(
            product['Код'], 1,
            data['nomenclature'],
            data['specifications']
        )
        
        if not materials_dict:
            await update.message.reply_text("❌ Нет материалов для этого изделия")
            return
        
        materials_list = []
        missing_prices = []
        saved_prices = get_all_material_prices()
        
        i = 1
        for m in materials_dict.values():
            raw = (m['baseQty'] / 1.5) * (session['efficiency'] / 100)
            rounded = (raw * 10 // 1 + 1) / 10 if raw * 10 % 1 > 0 else raw
            final_qty = rounded * drawings_needed
            materials_list.append({
                'number': i,
                'name': m['name'],
                'qty': final_qty,
                'price': saved_prices.get(m['name'], 0)
            })
            if m['name'] not in saved_prices or saved_prices[m['name']] == 0:
                missing_prices.append(m['name'])
            i += 1
        
        if missing_prices:
            session.update({
                'step': 'automatic_missing_prices',
                'qty': qty,
                'drawings_needed': drawings_needed,
                'materials_list': materials_list,
                'missing_prices': missing_prices
            })
            
            missing_text = "⚠️ *Для следующих материалов нет цен:*\n\n"
            for name in missing_prices:
                missing_text += f"• {name}\n"
            missing_text += f"\nВведите цены через пробел для этих {len(missing_prices)} материалов:"
            
            await update.message.reply_text(missing_text, parse_mode='Markdown')
        else:
            await calculate_and_send_result(update, session, materials_list, qty, drawings_needed)
        return
    
    # Шаг 5 (автоматический режим): Ввод недостающих цен
    elif session['step'] == 'automatic_missing_prices':
        parts = text.split()
        if len(parts) < len(session['missing_prices']):
            await update.message.reply_text(f"❌ Введите {len(session['missing_prices'])} цен через пробел")
            return
        
        try:
            prices = [float(p) for p in parts[:len(session['missing_prices'])]]
        except ValueError:
            await update.message.reply_text("❌ Введите числа")
            return
        
        price_index = 0
        for m in session['materials_list']:
            if m['name'] in session['missing_prices']:
                m['price'] = prices[price_index]
                save_material_price(m['name'], prices[price_index])
                price_index += 1
        
        await calculate_and_send_result(update, session, session['materials_list'], session['qty'], session['drawings_needed'])
        return

# ==================== ЗАПУСК ====================
def main():
    init_prices_db()
    
    # Создаем приложение с post_init
    app = Application.builder().token(TOKEN).post_init(post_init).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("nalog", nalog_command))
    app.add_handler(CommandHandler("categories", categories_command))
    app.add_handler(CommandHandler("automatic", automatic_command))
    app.add_handler(CommandHandler("instructions", instructions_command))
    
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    
    logger.info("✅ Бот запущен с новыми командами и защитой от чужих кнопок")
    
    # Запускаем бота
    app.run_polling()

if __name__ == "__main__":
    main()
