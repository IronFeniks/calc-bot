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

def format_materials_for_display(materials_list):
    """Формирует список материалов для отображения с ценами"""
    result = ""
    for m in materials_list:
        price_str = f"{format_number(m['price'])} ISK" if m['price'] > 0 else "не установлена"
        result += f"{m['number']}. {m['name']}: нужно {format_number(m['qty'])} шт | текущая цена: {price_str}\n"
    return result

def format_materials_short(materials_list):
    """Краткий список материалов для автоматического режима"""
    result = ""
    zero_prices = []
    for m in materials_list:
        if m['price'] == 0:
            zero_prices.append(f"{m['number']}. {m['name']} (нужно {format_number(m['qty'])} шт)")
    return zero_prices

def format_results(product_name, category, qty, efficiency, tax_rate, materials_list, result):
    """Формирует финальный отчет"""
    text = f"📊 *РЕЗУЛЬТАТЫ РАСЧЕТА*\n\n"
    text += f"Изделие: {product_name}\n"
    text += f"Категория: {category}\n"
    text += f"Количество: {qty:.0f} шт\n"
    text += f"Эффективность: {efficiency:.0f}%\n"
    text += f"Налог: {tax_rate:.0f}%\n\n"
    
    text += "*МАТЕРИАЛЫ:*\n"
    for m in materials_list:
        text += f"{m['number']}. {m['name']}: {format_number(m['qty'])} шт × {format_number(m['price'])} = {format_number(m['cost'])} ISK\n"
    
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
        text += f"\n📏 *НА 1 ШТУКУ:*\n"
        text += f"Себестоимость: {format_number(per_unit)} ISK\n"
        text += f"Прибыль: {format_number(per_unit_profit)} ISK\n"
    
    return text

def get_explanation_text():
    """Возвращает текст пояснения по расчетам"""
    return """
📖 *ПОЯСНИТЬ ПО ЦИФРАМ*

💰 *Материалы*
• Сумма всех затрат на материалы
• Рассчитывается как: (количество × цена за 1 шт) для каждого материала

🏭 *Производство*
• Фиксированная стоимость производства
• Берется из базы данных для выбранного изделия
• Умножается на количество чертежей

📄 *Чертежи*
• Стоимость разработки чертежей
• Включает: чертеж изделия + чертежи всех узлов
• Умножается на количество чертежей

💵 *Себестоимость*
• Материалы + Производство + Чертежи

📈 *Выручка*
• Рыночная цена × количество продукции

📊 *Прибыль до налога*
• Выручка − Себестоимость

💸 *Налог*
• Рассчитывается только при положительной прибыли
• Прибыль до налога × ставка налога / 100

✨ *Прибыль после налога*
• Прибыль до налога − Налог

📏 *НА 1 ШТУКУ*
• Все итоговые показатели делятся на количество продукции
• Показывает экономику единицы товара
"""

# ==================== ФУНКЦИИ ДЛЯ ОТОБРАЖЕНИЯ СТРАНИЦ ====================

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
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    
    if edit:
        await update_or_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await update_or_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def show_materials_page(update_or_query, session, edit: bool = False):
    """Показывает страницу с материалами и кнопками управления"""
    materials = session['materials_list']
    user_id = session.get('user_id')
    
    text = f"📋 *МАТЕРИАЛЫ ДЛЯ {session['product']['Наименование']}*\n\n"
    text += format_materials_for_display(materials)
    
    keyboard = [
        [InlineKeyboardButton("🔙 Назад", callback_data=f"user_{user_id}_back_to_products")],
        [InlineKeyboardButton("✏️ Ввод цен", callback_data=f"user_{user_id}_price_input")],
        [InlineKeyboardButton("🤖 Автоматически", callback_data=f"user_{user_id}_auto_prices")],
        [InlineKeyboardButton("❌ Отменить", callback_data="cancel")]
    ]
    
    if edit:
        await update_or_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await update_or_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# ==================== ОБРАБОТЧИКИ КОМАНД ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка команды /start с инструкцией"""
    if not await check_access(update):
        return
    
    instruction = """
👋 *ПРОИЗВОДСТВЕННЫЙ КАЛЬКУЛЯТОР*

📌 *Сохранение параметров*
• Эффективность и налог сохраняются на время сессии
• При смене категории нужно ввести заново

💾 *Сохранение цен*
• Цены материалов сохраняются автоматически
• При повторном расчете они подставляются

📋 *Процесс расчета*
1. Выберите категорию
2. Введите эффективность и налог
3. Выберите изделие по номеру
4. Введите цены и количество
5. Настройте цены материалов

❗ *БЛОКИРОВКА* ❗
• Ботом может пользоваться только один человек
• Если бот занят, вы увидите, кто именно

❓ *Вопросы и предложения*
Обращайтесь к администратору
"""
    
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
    
    # Показываем инструкцию и категории
    await update.message.reply_text(instruction, parse_mode='Markdown')
    
    categories_str = [str(cat) for cat in categories]
    keyboard = []
    for cat in sorted(categories_str):
        callback_data = f"user_{user_id}_cat_{cat}"
        keyboard.append([InlineKeyboardButton(cat, callback_data=callback_data)])
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    
    await update.message.reply_text(
        "📋 *Выберите категорию:*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

# ==================== ОБРАБОТЧИК КНОПОК ====================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    data = query.data
    
    # Проверяем, что callback data содержит ID пользователя
    if not data.startswith(f"user_{user_id}_") and data not in ["cancel"]:
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
        if bot_lock.current_user == user_id:
            bot_lock.release(user_id)
            sessions.pop(user_id, None)
            await query.edit_message_text("❌ Расчет отменен")
        else:
            await query.answer("⛔ Вы не можете отменить чужой расчет", show_alert=True)
        return
    
    if clean_data == "back_to_categories":
        # Возврат к категориям
        session = sessions.get(user_id, {})
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
            "📋 *Выберите категорию:*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return
    
    if clean_data == "back_to_products":
        # Возврат к выбору изделия
        session = sessions.get(user_id)
        if session:
            session['step'] = 'product_selection'
            session['product_page'] = 0
            await show_products_page(query, session, edit=True)
        return
    
    if clean_data == "price_input":
        # Начинаем пошаговый ввод цен
        session = sessions.get(user_id)
        if session:
            session['step'] = 'price_input'
            session['current_material'] = 0
            await process_next_material_price(query, session)
        return
    
    if clean_data == "auto_prices":
        # Автоматическая подстановка цен
        session = sessions.get(user_id)
        if session:
            # Подставляем цены из базы
            saved_prices = get_all_material_prices()
            zero_materials = []
            for m in session['materials_list']:
                saved = saved_prices.get(m['name'], 0)
                m['price'] = saved
                if saved == 0:
                    zero_materials.append(m)
            
            if zero_materials:
                # Есть материалы с нулевой ценой
                zero_list = "\n".join([f"{m['number']}. {m['name']} (нужно {format_number(m['qty'])} шт)" for m in zero_materials])
                text = f"✅ *Цены подставлены автоматически*\n\n*Материалы с нулевой ценой:*\n{zero_list}"
                keyboard = [
                    [InlineKeyboardButton("▶️ Продолжить", callback_data=f"user_{user_id}_continue")],
                    [InlineKeyboardButton("✏️ Ввести недостающие", callback_data=f"user_{user_id}_price_input")],
                    [InlineKeyboardButton("❌ Отменить", callback_data="cancel")]
                ]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            else:
                # Все цены есть - сразу продолжаем
                await continue_to_result(query, session)
        return
    
    if clean_data == "continue":
        session = sessions.get(user_id)
        if session:
            await continue_to_result(query, session)
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
    
    if clean_data.startswith("cat_"):
        category = clean_data[4:]
        sessions[user_id] = {'step': 'parameters', 'category': category, 'user_id': user_id}
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]
        await query.edit_message_text(
            f"📊 *Параметры для категории {category}*\n\n"
            f"Введите через пробел:\n"
            f"`Эффективность (%) Налог (%)`\n\n"
            f"Пример: `150 20`",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return

async def process_next_material_price(update_or_query, session):
    """Обрабатывает пошаговый ввод цен материалов"""
    user_id = session['user_id']
    materials = session['materials_list']
    current = session.get('current_material', 0)
    
    if current >= len(materials):
        # Все материалы обработаны
        await continue_to_result(update_or_query, session)
        return
    
    m = materials[current]
    saved_prices = get_all_material_prices()
    current_price = m.get('price', saved_prices.get(m['name'], 0))
    
    text = f"📦 *Материал {current + 1} из {len(materials)}*\n\n"
    text += f"*{m['name']}*\n"
    text += f"Необходимое количество: {format_number(m['qty'])} шт\n"
    text += f"Текущая цена в базе: {format_number(current_price)} ISK\n\n"
    text += f"Введите цену для {m['name']} (или 0 если цена не нужна):"
    
    keyboard = [[InlineKeyboardButton("❌ Отменить", callback_data="cancel")]]
    
    if hasattr(update_or_query, 'message'):
        await update_or_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await update_or_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    session['step'] = 'price_input_waiting'

async def continue_to_result(update_or_query, session):
    """Переход к финальному расчету"""
    user_id = session['user_id']
    
    # Рассчитываем итоги
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
    
    # Формируем результат
    result_text = format_results(
        session['product']['Наименование'],
        session['category'],
        session['qty'],
        session['efficiency'],
        session['tax'],
        session['materials_list'],
        result
    )
    
    # Кнопки для финального результата
    keyboard = [
        [
            InlineKeyboardButton("🔄 Новый расчет", callback_data=f"user_{user_id}_restart"),
            InlineKeyboardButton("📂 Та же категория", callback_data=f"user_{user_id}_same_category")
        ],
        [
            InlineKeyboardButton("📋 Копировать", callback_data=f"user_{user_id}_copy"),
            InlineKeyboardButton("📖 Пояснить по цифрам", callback_data=f"user_{user_id}_explain")
        ]
    ]
    
    if hasattr(update_or_query, 'message'):
        await update_or_query.message.reply_text(result_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await update_or_query.edit_message_text(result_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    # Не очищаем сессию, чтобы можно было сделать "Та же категория"

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
    
    # Обработка ввода параметров (эффективность и налог)
    if session['step'] == 'parameters':
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text("❌ Введите через пробел: Эффективность Налог\nПример: 150 20")
            return
        
        try:
            efficiency = float(parts[0].replace(',', '.'))
            tax = float(parts[1].replace(',', '.'))
        except ValueError:
            await update.message.reply_text("❌ Введите числа")
            return
        
        session.update({
            'step': 'product_selection',
            'efficiency': efficiency,
            'tax': tax
        })
        
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
            'products': products,
            'product_page': 0
        })
        await show_products_page(update, session, edit=False)
        return
    
    # Обработка выбора изделия
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
        product = None
        for item in data['nomenclature']:
            if item['Код'] == selected['code']:
                product = item
                break
        
        if not product:
            await update.message.reply_text("❌ Ошибка получения данных")
            return
        
        # Получаем кратность
        try:
            multiplicity = product.get('Кратность', 1)
            if multiplicity is None or multiplicity == '' or (isinstance(multiplicity, float) and pd.isna(multiplicity)):
                multiplicity = 1
            else:
                multiplicity = int(float(multiplicity))
        except:
            multiplicity = 1
        
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
    
    # Обработка ввода цен
    elif session['step'] == 'prices':
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text("❌ Введите две цены через пробел\nПример: 3200000 6900000")
            return
        
        try:
            market_price = float(parts[0].replace(',', '.'))
            drawing_price = float(parts[1].replace(',', '.'))
        except ValueError:
            await update.message.reply_text("❌ Введите числа")
            return
        
        save_drawing_price(session['product']['Код'], drawing_price)
        session.update({
            'market_price': market_price,
            'drawing_price': drawing_price,
            'step': 'quantity'
        })
        await update.message.reply_text("📦 Введите количество продукции (шт):")
        return
    
    # Обработка ввода количества
    elif session['step'] == 'quantity':
        try:
            qty = float(text.replace(',', '.'))
        except ValueError:
            await update.message.reply_text("❌ Введите число")
            return
        
        product = session['product']
        output = session['output_per_drawing']
        
        if qty % output != 0:
            await update.message.reply_text(f"⚠️ Количество должно быть кратно {output}")
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
        
        # Формируем список материалов
        materials_list = []
        i = 1
        for m in materials_dict.values():
            raw = (m['baseQty'] / 1.5) * (session['efficiency'] / 100)
            rounded = (raw * 10 // 1 + 1) / 10 if raw * 10 % 1 > 0 else raw
            final_qty = rounded * drawings_needed
            materials_list.append({
                'number': i,
                'name': m['name'],
                'qty': final_qty,
                'price': 0,
                'cost': 0
            })
            i += 1
        
        saved_prices = get_all_material_prices()
        for m in materials_list:
            if m['name'] in saved_prices:
                m['price'] = saved_prices[m['name']]
        
        session.update({
            'step': 'materials',
            'qty': qty,
            'drawings_needed': drawings_needed,
            'materials_list': materials_list
        })
        
        await show_materials_page(update, session, edit=False)
        return
    
    # Обработка пошагового ввода цен материалов
    elif session['step'] == 'price_input_waiting':
        try:
            price = float(text.replace(',', '.'))
        except ValueError:
            await update.message.reply_text("❌ Введите число")
            return
        
        current = session.get('current_material', 0)
        materials = session['materials_list']
        
        if current < len(materials):
            materials[current]['price'] = price
            materials[current]['cost'] = price * materials[current]['qty']
            save_material_price(materials[current]['name'], price)
            
            session['current_material'] = current + 1
            await process_next_material_price(update, session)
        return

# ==================== ДОПОЛНИТЕЛЬНЫЕ ОБРАБОТЧИКИ ====================
async def button_handler_extended(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Дополнительные обработчики кнопок (вынесено для читаемости)"""
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    
    # Очищаем data от префикса с user_id
    if data.startswith(f"user_{user_id}_"):
        clean_data = data.replace(f"user_{user_id}_", "")
    else:
        return
    
    if clean_data == "restart":
        # Полностью новый расчет
        bot_lock.release(user_id)
        sessions.pop(user_id, None)
        await start(update, context)
        return
    
    if clean_data == "same_category":
        # Новый расчет в той же категории
        session = sessions.get(user_id)
        if session:
            # Очищаем данные, но сохраняем категорию
            new_session = {
                'user_id': user_id,
                'category': session['category'],
                'step': 'parameters'
            }
            sessions[user_id] = new_session
            keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]]
            await query.edit_message_text(
                f"📊 *Параметры для категории {session['category']}*\n\n"
                f"Введите через пробел:\n"
                f"`Эффективность (%) Налог (%)`\n\n"
                f"Пример: `150 20`",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        return
    
    if clean_data == "copy":
        # Копирование результатов
        await query.answer("📋 Результаты скопированы в буфер обмена (имитация)", show_alert=True)
        return
    
    if clean_data == "explain":
        # Показать пояснение
        keyboard = [[InlineKeyboardButton("🔙 Назад к результатам", callback_data=f"user_{user_id}_back_to_result")]]
        await query.edit_message_text(
            get_explanation_text(),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return
    
    if clean_data == "back_to_result":
        # Вернуться к результатам
        session = sessions.get(user_id)
        if session and 'last_result' in session:
            # Здесь нужно восстановить последний результат
            await query.edit_message_text(
                session['last_result'],
                reply_markup=session['last_keyboard'],
                parse_mode='Markdown'
            )
        return

# ==================== ЗАПУСК ====================
def main():
    init_prices_db()
    
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(CallbackQueryHandler(button_handler_extended))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    
    logger.info("✅ Бот запущен с новой логикой")
    app.run_polling()

if __name__ == "__main__":
    main()
