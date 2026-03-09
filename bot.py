import logging
import time
import pandas as pd
import requests
import sqlite3
import os
from io import BytesIO
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Message, Chat, User
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
        if self.current_user is None or (time.time() - self.lock_time) > 300:  # 5 минут
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
    
    def check_timeout(self):
        """Проверяет, не истек ли таймаут"""
        if self.current_user and (time.time() - self.lock_time) > 300:
            logger.info(f"⏰ Таймаут для пользователя {self.current_user}")
            self.current_user = None
            self.username = None
            self.first_name = None
            return True
        return False

bot_lock = UserLock()

# ==================== ХРАНИЛИЩЕ ДАННЫХ ====================
cached_data = None
last_update = 0
sessions = {}

# ==================== ЗАГРУЗКА С YANDEX ДИСКА ====================
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
    
    # Проверяем различные источники thread_id
    topic_id = None
    
    # Если есть сообщение
    if update.message:
        topic_id = update.message.message_thread_id
        logger.info(f"Из сообщения: thread_id={topic_id}")
    
    # Если есть callback query
    elif update.callback_query and update.callback_query.message:
        topic_id = update.callback_query.message.message_thread_id
        logger.info(f"Из callback: thread_id={topic_id}")
    
    logger.info(f"Проверка доступа: chat={chat_id}, topic={topic_id}, required_topic={TOPIC_ID}")
    
    # Для группы с топиками проверяем совпадение
    if chat_id == GROUP_ID:
        # Если топик не указан (None), значит сообщение в общем чате, а не в топике
        if topic_id is None:
            logger.warning(f"Сообщение в общем чате, а не в топике")
            return False
        # Проверяем совпадение с требуемым топиком
        if topic_id == TOPIC_ID:
            return True
        else:
            logger.warning(f"Неверный топик: {topic_id}, требуется: {TOPIC_ID}")
            return False
    
    # Если это не группа или группа без ограничений
    return True

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
        result += f"{m['number']}. {m['name']}: нужно {format_number(m['qty'])} шт | цена: {price_str}\n"
    return result

def format_materials_short(materials_list):
    """Краткий список материалов для автоматического режима"""
    result = ""
    zero_prices = []
    for m in materials_list:
        if m['price'] == 0:
            zero_prices.append(f"{m['number']}. {m['name']} (нужно {format_number(m['qty'])} шт)")
    return zero_prices

def format_results(product_name, category_path, qty, efficiency, tax_rate, materials_list, result):
    """Формирует финальный отчет"""
    text = f"📊 *РЕЗУЛЬТАТЫ РАСЧЕТА*\n\n"
    text += f"Изделие: {product_name}\n"
    text += f"Категория: {' > '.join(category_path)}\n"
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
📖 *ПОЯСНЕНИЕ ПО ЦИФРАМ*

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

# ==================== ФУНКЦИИ ДЛЯ РАБОТЫ С КАТЕГОРИЯМИ ====================

def parse_category_path(category_str):
    """Разбирает строку категории на путь, учитывая пробелы вокруг >"""
    if pd.isna(category_str) or not category_str:
        return []
    # Разделяем по " > " и удаляем лишние пробелы
    return [cat.strip() for cat in str(category_str).split(' > ')]

def build_category_tree(nomenclature):
    """Строит дерево категорий из номенклатуры"""
    tree = {}
    
    for item in nomenclature:
        category_str = item.get('Категории')
        if pd.isna(category_str) or not category_str:
            continue
            
        path = parse_category_path(category_str)
        if not path:
            continue
            
        logger.info(f"Обработка пути: {path} для изделия {item.get('Наименование')}")
        
        current = tree
        # Проходим по всем уровням пути
        for i, cat in enumerate(path):
            if cat not in current:
                current[cat] = {'_subcategories': {}, '_items': []}
                logger.info(f"  Добавлена категория: {cat}")
            
            # Если это последний уровень - добавляем изделие
            if i == len(path) - 1:
                item_type = str(item.get('Тип') or '').lower()
                if 'изделие' in item_type or 'узел' in item_type:
                    # Проверяем, нет ли уже такого изделия
                    exists = False
                    for existing in current[cat]['_items']:
                        if existing['code'] == item['Код']:
                            exists = True
                            break
                    if not exists:
                        current[cat]['_items'].append({
                            'code': item['Код'],
                            'name': item['Наименование']
                        })
                        logger.info(f"  Добавлено изделие {item['Наименование']} в категорию {cat}")
            
            current = current[cat]['_subcategories']
    
    # Логируем итоговую структуру
    logger.info("Итоговая структура дерева:")
    for cat, content in tree.items():
        logger.info(f"  {cat}: подкатегории={list(content['_subcategories'].keys())}, изделий={len(content['_items'])}")
    
    return tree

def get_categories_at_level(tree, path=None):
    """Возвращает список подкатегорий на указанном уровне"""
    if path is None:
        path = []
    
    current = tree
    for cat in path:
        if cat in current:
            current = current[cat]['_subcategories']
        else:
            return []
    
    return list(current.keys())

def get_items_at_level(tree, path):
    """Возвращает список изделий на указанном уровне"""
    if not path:
        return []
    
    current = tree
    # Проходим по всему пути
    for cat in path:
        if cat in current:
            current = current[cat]['_subcategories']
        else:
            logger.warning(f"Категория {cat} не найдена в пути {path}")
            return []
    
    # Возвращаем изделия с этого уровня
    items = []
    
    # Проверяем, есть ли изделия в текущей категории
    last_cat = path[-1]
    temp = tree
    for cat in path:
        if cat in temp:
            if cat == last_cat:
                # Это последняя категория - берем ее изделия
                if '_items' in temp[cat]:
                    items = temp[cat]['_items']
                    logger.info(f"Найдено изделий в {last_cat}: {len(items)}")
            temp = temp[cat]['_subcategories']
    
    return items

# ==================== ФУНКЦИИ ДЛЯ СОЗДАНИЯ КЛАВИАТУР ====================

def get_back_cancel_keyboard(user_id, back_callback=None):
    """Создает клавиатуру с кнопками Назад и Отмена"""
    keyboard = []
    row = []
    
    if back_callback:
        row.append(InlineKeyboardButton("🔙 Назад", callback_data=f"user_{user_id}_{back_callback}"))
    
    row.append(InlineKeyboardButton("❌ Отмена", callback_data="cancel"))
    
    if row:
        keyboard.append(row)
    
    return InlineKeyboardMarkup(keyboard) if keyboard else None

def get_navigation_keyboard(user_id, show_back=True, show_cancel=True, back_callback=None):
    """Создает клавиатуру с навигационными кнопками"""
    keyboard = []
    nav_row = []
    
    if show_back and back_callback:
        nav_row.append(InlineKeyboardButton("🔙 Назад", callback_data=f"user_{user_id}_{back_callback}"))
    
    if show_cancel:
        nav_row.append(InlineKeyboardButton("❌ Отмена", callback_data="cancel"))
    
    if nav_row:
        keyboard.append(nav_row)
    
    return InlineKeyboardMarkup(keyboard) if keyboard else None

# ==================== ФУНКЦИИ ДЛЯ ОТОБРАЖЕНИЯ СТРАНИЦ ====================

async def show_categories_page(update_or_query, session, edit: bool = False):
    """Показывает категории на текущем уровне"""
    user_id = session['user_id']
    tree = session['category_tree']
    path = session.get('category_path', [])
    
    logger.info(f"Показ категорий: путь={path}")
    
    # Получаем категории на текущем уровне
    categories = get_categories_at_level(tree, path)
    logger.info(f"Найденные категории: {categories}")
    
    if not categories:
        # Если нет подкатегорий, проверяем есть ли изделия
        products = get_items_at_level(tree, path)
        if products:
            # Если есть изделия, показываем их
            session['products'] = products
            session['product_page'] = 0
            await show_products_page(update_or_query, session, edit)
        else:
            # Если ничего нет
            text = "❌ В этой категории нет элементов"
            if edit:
                await update_or_query.message.edit_text(text)
            else:
                await update_or_query.message.reply_text(text)
        return
    
    # Формируем текст
    if path:
        text = f"📂 *{' > '.join(path)}*\n\nВыберите подкатегорию:"
    else:
        text = "📋 *Выберите категорию:*"
    
    # Создаем клавиатуру
    keyboard = []
    for cat in sorted(categories):
        callback_data = f"user_{user_id}_cat_{cat}"
        keyboard.append([InlineKeyboardButton(f"📁 {cat}", callback_data=callback_data)])
    
    # Кнопки навигации
    nav_row = []
    if path:
        nav_row.append(InlineKeyboardButton("🔙 Назад", callback_data=f"user_{user_id}_back_to_categories"))
    nav_row.append(InlineKeyboardButton("❌ Отмена", callback_data="cancel"))
    keyboard.append(nav_row)
    
    if edit:
        await update_or_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await update_or_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def show_products_page(update_or_query, session, edit: bool = False):
    """Показывает список изделий в текущей категории"""
    products = session['products']
    page = session.get('product_page', 0)
    items_per_page = 20
    user_id = session.get('user_id')
    path = session.get('category_path', [])
    
    if not user_id and hasattr(update_or_query, 'from_user'):
        user_id = update_or_query.from_user.id
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(products))
    total_pages = (len(products) - 1) // items_per_page + 1
    
    text = f"📋 *{' > '.join(path)}*\n\n"
    text += f"*Доступные изделия (страница {page + 1}/{total_pages}):*\n\n"
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
    
    # Кнопки навигации
    back_cancel_row = []
    back_cancel_row.append(InlineKeyboardButton("🔙 Назад к категориям", callback_data=f"user_{user_id}_back_to_categories"))
    back_cancel_row.append(InlineKeyboardButton("❌ Отмена", callback_data="cancel"))
    keyboard.append(back_cancel_row)
    
    if edit:
        await update_or_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await update_or_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def show_materials_page(update_or_query, session, edit: bool = False, page: int = 0):
    """Показывает страницу с материалами и кнопками управления"""
    materials = session['materials_list']
    user_id = session.get('user_id')
    path = session.get('category_path', [])
    
    items_per_page = 10
    total_pages = (len(materials) + items_per_page - 1) // items_per_page
    
    # Проверяем, что запрошенная страница существует
    if page >= total_pages:
        page = total_pages - 1
    if page < 0:
        page = 0
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(materials))
    
    text = f"📋 *{' > '.join(path)}*\n\n"
    text += f"*МАТЕРИАЛЫ ДЛЯ {session['product']['Наименование']}*\n\n"
    
    if total_pages > 1:
        text += f"*Страница {page + 1} из {total_pages}*\n\n"
    
    for i in range(start_idx, end_idx):
        m = materials[i]
        price_str = f"{format_number(m['price'])} ISK" if m['price'] > 0 else "не установлена"
        text += f"{m['number']}. {m['name']}: нужно {format_number(m['qty'])} шт | цена: {price_str}\n"
    
    # Создаем клавиатуру
    keyboard = []
    
    # Кнопки навигации по страницам
    if len(materials) > 15 and total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("◀️ Назад", callback_data=f"user_{user_id}_materials_page_{page-1}"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"user_{user_id}_materials_page_{page+1}"))
        if nav_row:
            keyboard.append(nav_row)
    
    # Основные кнопки действий
    keyboard.append([InlineKeyboardButton("✏️ Ввод цен", callback_data=f"user_{user_id}_price_input")])
    keyboard.append([InlineKeyboardButton("🤖 Автоматически", callback_data=f"user_{user_id}_auto_prices")])
    
    # Кнопки навигации
    nav_row = []
    nav_row.append(InlineKeyboardButton("🔙 Назад к выбору изделия", callback_data=f"user_{user_id}_back_to_products"))
    nav_row.append(InlineKeyboardButton("❌ Отмена", callback_data="cancel"))
    keyboard.append(nav_row)
    
    if edit:
        await update_or_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await update_or_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    # Сохраняем текущую страницу в сессии
    session['materials_page'] = page

# ==================== ОБРАБОТЧИК КОМАНД ====================

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
    
    data = load_from_yandex()
    
    if not data['nomenclature']:
        await update.message.reply_text("❌ Ошибка загрузки данных")
        bot_lock.release(user_id)
        return
    
    # Выводим пример первых нескольких записей для отладки
    logger.info("Пример первых записей номенклатуры:")
    for i, item in enumerate(data['nomenclature'][:5]):
        cat = item.get('Категории', '')
        logger.info(f"  Запись {i+1}: Категории='{cat}', Наименование='{item.get('Наименование')}', Тип='{item.get('Тип')}'")
    
    # Строим дерево категорий
    category_tree = build_category_tree(data['nomenclature'])
    
    if not category_tree:
        await update.message.reply_text("❌ В базе нет категорий")
        bot_lock.release(user_id)
        return
    
    sessions[user_id] = {
        'step': 'categories',
        'user_id': user_id,
        'category_tree': category_tree,
        'category_path': []
    }
    
    # Показываем инструкцию и категории
    await update.message.reply_text(instruction, parse_mode='Markdown')
    await show_categories_page(update, sessions[user_id], edit=False)

# ==================== ОБРАБОТЧИК КНОПОК (ОБЪЕДИНЕННЫЙ) ====================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    # Проверяем таймаут
    if bot_lock.check_timeout():
        sessions.pop(user_id, None)
        await query.edit_message_text("⏰ *Сессия завершена из-за бездействия*\n\nИспользуйте /start для нового расчета", parse_mode='Markdown')
        return
    
    data = query.data
    logger.info(f"Обработка callback: {data} от пользователя {user_id}")
    
    # Проверяем, что callback data содержит ID пользователя (кроме глобальной отмены)
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
    
    # Глобальная отмена
    if clean_data == "cancel":
        if bot_lock.current_user == user_id:
            bot_lock.release(user_id)
            sessions.pop(user_id, None)
            await query.edit_message_text("❌ Расчет отменен")
        else:
            await query.answer("⛔ Вы не можете отменить чужой расчет", show_alert=True)
        return
    
    # ==================== ФИНАЛЬНЫЕ КНОПКИ ====================
    if clean_data == "restart":
        # Полностью новый расчет
        bot_lock.release(user_id)
        sessions.pop(user_id, None)
        
        # Создаем новое сообщение
        await query.message.reply_text("🔄 *Начинаем новый расчет...*", parse_mode='Markdown')
        
        # Вызываем start через создание нового update
        await restart_bot(query, context)
        return
    
    if clean_data == "same_category":
        # Новый расчет в той же категории
        session = sessions.get(user_id)
        if session and 'category_path' in session:
            # Очищаем данные, но сохраняем путь категории
            new_session = {
                'user_id': user_id,
                'category_tree': session['category_tree'],
                'category_path': session['category_path'].copy(),
                'step': 'parameters'
            }
            sessions[user_id] = new_session
            path_str = ' > '.join(session['category_path'])
            keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_categories")
            await query.edit_message_text(
                f"📊 *Параметры для категории {path_str}*\n\n"
                f"Введите через пробел:\n"
                f"`Эффективность (%) Налог (%)`\n\n"
                f"Пример: `150 20`",
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
        else:
            await query.answer("❌ Не удалось определить категорию", show_alert=True)
        return
    
    if clean_data == "copy":
        # Копирование результатов - просто уведомление
        await query.answer("📋 Текст результата можно скопировать выделением", show_alert=True)
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
        if session and 'last_result' in session and 'last_keyboard' in session:
            await query.edit_message_text(
                session['last_result'],
                reply_markup=InlineKeyboardMarkup(session['last_keyboard']),
                parse_mode='Markdown'
            )
        else:
            await query.answer("❌ Результаты не найдены", show_alert=True)
        return
    
    # ==================== КНОПКИ НАВИГАЦИИ ====================
    if clean_data == "back_to_categories":
        # Возврат к категориям
        session = sessions.get(user_id)
        if session:
            if session.get('category_path'):
                # Убираем последнюю категорию из пути
                session['category_path'].pop()
                logger.info(f"Возврат к категориям, новый путь: {session['category_path']}")
            
            session['step'] = 'categories'
            await show_categories_page(query, session, edit=True)
        return
    
    if clean_data == "back_to_parameters":
        # Возврат к вводу параметров
        session = sessions.get(user_id)
        if session and 'category_path' in session:
            session['step'] = 'parameters'
            path_str = ' > '.join(session['category_path'])
            keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_categories")
            await query.edit_message_text(
                f"📊 *Параметры для категории {path_str}*\n\n"
                f"Введите через пробел:\n"
                f"`Эффективность (%) Налог (%)`\n\n"
                f"Пример: `150 20`",
                reply_markup=keyboard,
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
    
    if clean_data == "back_to_prices":
        # Возврат к вводу цен
        session = sessions.get(user_id)
        if session:
            session['step'] = 'prices'
            saved_price = get_drawing_price(session['product']['Код'])
            price_text = f"✅ Выбрано: *{session['product']['Наименование']}*\nКратность: {session['output_per_drawing']}\n\n"
            price_text += f"💰 Введите через пробел:\n`Рыночная цена Стоимость чертежа`\n\n"
            if saved_price > 0:
                price_text += f"*(сохранённая стоимость чертежа: {format_number(saved_price)} ISK)*\n"
            price_text += f"Пример: `3200000 6900000`"
            
            keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_products")
            await query.edit_message_text(price_text, parse_mode='Markdown', reply_markup=keyboard)
        return
    
    if clean_data == "back_to_materials":
        # Возврат к списку материалов
        session = sessions.get(user_id)
        if session:
            session['step'] = 'materials'
            await show_materials_page(query, session, edit=True, page=session.get('materials_page', 0))
        return
    
    # ==================== НАВИГАЦИЯ ПО МАТЕРИАЛАМ ====================
    if clean_data.startswith("materials_page_"):
        try:
            page = int(clean_data.replace("materials_page_", ""))
            session = sessions.get(user_id)
            if session:
                await show_materials_page(query, session, edit=True, page=page)
            else:
                await query.answer("❌ Сессия не найдена", show_alert=True)
        except Exception as e:
            logger.error(f"Ошибка пагинации: {e}")
            await query.answer("❌ Ошибка навигации", show_alert=True)
        return
    
    # ==================== ОСНОВНЫЕ КНОПКИ ====================
    if clean_data == "price_input":
        # Начинаем пошаговый ввод цен
        session = sessions.get(user_id)
        if session:
            session['step'] = 'price_input'
            session['current_material'] = 0
            await process_next_material_price(query, session)
        return
    
    if clean_data == "price_input_missing":
        # Ввод только недостающих цен
        session = sessions.get(user_id)
        if session:
            # Создаем список материалов без цен
            missing_materials = []
            for i, m in enumerate(session['materials_list']):
                if m['price'] == 0:
                    missing_materials.append({
                        'index': i,
                        'name': m['name'],
                        'qty': m['qty']
                    })
            
            session['missing_materials'] = missing_materials
            session['current_missing_index'] = 0
            session['step'] = 'price_input_missing'
            await process_next_missing_price(query, session)
        return
    
    if clean_data == "auto_prices":
        # Автоматическая подстановка цен
        session = sessions.get(user_id)
        if session:
            # Подставляем цены из базы
            saved_prices = get_all_material_prices()
            materials_with_price = []
            materials_without_price = []
            
            for m in session['materials_list']:
                saved = saved_prices.get(m['name'], 0)
                m['price'] = saved
                if saved > 0:
                    materials_with_price.append(m)
                else:
                    materials_without_price.append(m)
            
            if materials_without_price:
                # Есть материалы без цен
                without_list = "\n".join([f"{m['number']}. {m['name']} (нужно {format_number(m['qty'])} шт)" for m in materials_without_price])
                text = f"✅ *Цены подставлены автоматически*\n\n"
                text += f"*Материалы с ценами:* {len(materials_with_price)} шт\n"
                text += f"*Материалы без цен:* {len(materials_without_price)} шт\n\n"
                text += f"*Нужно ввести цены для:*\n{without_list}"
                
                keyboard = [
                    [InlineKeyboardButton("▶️ Продолжить с имеющимися ценами", callback_data=f"user_{user_id}_continue")],
                    [InlineKeyboardButton("✏️ Ввести недостающие", callback_data=f"user_{user_id}_price_input_missing")],
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
        session = sessions.get(user_id)
        
        if session:
            # Добавляем категорию в путь
            if 'category_path' not in session:
                session['category_path'] = []
            
            # Добавляем выбранную категорию
            session['category_path'].append(category)
            logger.info(f"Текущий путь: {session['category_path']}")
            
            # Проверяем, есть ли подкатегории на этом уровне
            next_categories = get_categories_at_level(session['category_tree'], session['category_path'])
            logger.info(f"Найдены подкатегории: {next_categories}")
            
            # Проверяем, есть ли изделия на этом уровне
            products = get_items_at_level(session['category_tree'], session['category_path'])
            logger.info(f"Найдены изделия: {len(products) if products else 0}")
            
            if next_categories:
                # Есть подкатегории - показываем их
                logger.info(f"Показываем подкатегории: {next_categories}")
                session['step'] = 'categories'
                await show_categories_page(query, session, edit=True)
            elif products:
                # Нет подкатегорий, но есть изделия - переходим к параметрам
                logger.info(f"Показываем изделия: {len(products)}")
                session.update({
                    'step': 'parameters',
                    'products': products,
                    'product_page': 0
                })
                
                # Запрашиваем параметры
                path_str = ' > '.join(session['category_path'])
                keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_categories")
                await query.edit_message_text(
                    f"📊 *Параметры для категории {path_str}*\n\n"
                    f"Введите через пробел:\n"
                    f"`Эффективность (%) Налог (%)`\n\n"
                    f"Пример: `150 20`",
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
            else:
                # Нет ни подкатегорий, ни изделий
                await query.answer("❌ В этой категории нет изделий или подкатегорий", show_alert=True)
                # Убираем последнюю добавленную категорию из пути
                session['category_path'].pop()
        return
    
    logger.warning(f"Неизвестный callback: {clean_data}")

async def restart_bot(query, context: ContextTypes.DEFAULT_TYPE):
    """Вспомогательная функция для перезапуска бота"""
    thread_id = query.message.message_thread_id
    logger.info(f"Перезапуск бота с thread_id={thread_id}")
    
    # Создаем искусственное сообщение с правильным thread_id
    message = Message(
        message_id=query.message.message_id,
        date=query.message.date,
        chat=query.message.chat,
        from_user=query.from_user,
        text="/start",
        message_thread_id=thread_id
    )
    
    # Создаем новый update
    new_update = Update(update_id=0, message=message)
    
    # Вызываем start - thread_id уже содержится в message.message_thread_id
    await start(new_update, context)

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
    
    keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_materials")
    
    if hasattr(update_or_query, 'message'):
        await update_or_query.message.reply_text(text, reply_markup=keyboard, parse_mode='Markdown')
    else:
        await update_or_query.edit_message_text(text, reply_markup=keyboard, parse_mode='Markdown')
    
    session['step'] = 'price_input_waiting'

async def process_next_missing_price(update_or_query, session):
    """Обрабатывает пошаговый ввод только недостающих цен материалов"""
    user_id = session['user_id']
    missing_materials = session.get('missing_materials', [])
    current = session.get('current_missing_index', 0)
    
    if current >= len(missing_materials):
        # Все недостающие материалы обработаны
        await continue_to_result(update_or_query, session)
        return
    
    missing = missing_materials[current]
    material = session['materials_list'][missing['index']]
    
    text = f"📦 *Ввод недостающих цен ({current + 1} из {len(missing_materials)})*\n\n"
    text += f"*{material['name']}*\n"
    text += f"Необходимое количество: {format_number(material['qty'])} шт\n\n"
    text += f"Введите цену для {material['name']}:"
    
    keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_materials")
    
    if hasattr(update_or_query, 'message'):
        await update_or_query.message.reply_text(text, reply_markup=keyboard, parse_mode='Markdown')
    else:
        await update_or_query.edit_message_text(text, reply_markup=keyboard, parse_mode='Markdown')
    
    session['step'] = 'price_input_missing_waiting'

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
        session['category_path'],
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
    
    # Сохраняем результат в сессии для возврата из пояснений
    session['last_result'] = result_text
    session['last_keyboard'] = keyboard
    
    if hasattr(update_or_query, 'message'):
        await update_or_query.message.reply_text(result_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await update_or_query.edit_message_text(result_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

# ==================== ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ ====================
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update):
        return
    
    user_id = update.effective_user.id
    
    # Проверяем таймаут
    if bot_lock.check_timeout():
        sessions.pop(user_id, None)
        await update.message.reply_text("⏰ *Сессия завершена из-за бездействия*\n\nИспользуйте /start для нового расчета", parse_mode='Markdown')
        return
    
    session = sessions.get(user_id)
    
    if not session:
        await update.message.reply_text("Используйте /start")
        return
    
    if bot_lock.is_locked() and bot_lock.current_user != user_id:
        lock_info = bot_lock.get_lock_info()
        name = lock_info['first_name'] or f"@{lock_info['username']}" if lock_info['username'] else f"ID {lock_info['user_id']}"
        await update.message.reply_text(f"⏳ *Бот занят*\n\nСейчас расчёты выполняет: *{name}*", parse_mode='Markdown')
        return
    
    # Обновляем время активности
    bot_lock.lock_time = time.time()
    
    text = update.message.text
    logger.info(f"Текст от {user_id}: {text}, шаг: {session.get('step')}")
    
    # Обработка ввода параметров (эффективность и налог)
    if session['step'] == 'parameters':
        parts = text.split()
        if len(parts) < 2:
            keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_categories")
            await update.message.reply_text(
                "❌ Введите через пробел: Эффективность Налог\nПример: 150 20",
                reply_markup=keyboard
            )
            return
        
        try:
            efficiency = float(parts[0].replace(',', '.'))
            tax = float(parts[1].replace(',', '.'))
        except ValueError:
            keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_categories")
            await update.message.reply_text(
                "❌ Введите числа",
                reply_markup=keyboard
            )
            return
        
        session.update({
            'step': 'product_selection',
            'efficiency': efficiency,
            'tax': tax
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
            keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_parameters")
            await update.message.reply_text(
                f"❌ Введите число от 1 до {len(session['products'])}",
                reply_markup=keyboard
            )
            return
        
        data = load_from_yandex()
        product = None
        for item in data['nomenclature']:
            if item['Код'] == selected['code']:
                product = item
                break
        
        if not product:
            keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_parameters")
            await update.message.reply_text(
                "❌ Ошибка получения данных",
                reply_markup=keyboard
            )
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
        
        keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_parameters")
        await update.message.reply_text(price_text, parse_mode='Markdown', reply_markup=keyboard)
        return
    
    # Обработка ввода цен
    elif session['step'] == 'prices':
        parts = text.split()
        if len(parts) < 2:
            keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_products")
            await update.message.reply_text(
                "❌ Введите две цены через пробел\nПример: 3200000 6900000",
                reply_markup=keyboard
            )
            return
        
        try:
            market_price = float(parts[0].replace(',', '.'))
            drawing_price = float(parts[1].replace(',', '.'))
        except ValueError:
            keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_products")
            await update.message.reply_text(
                "❌ Введите числа",
                reply_markup=keyboard
            )
            return
        
        save_drawing_price(session['product']['Код'], drawing_price)
        session.update({
            'market_price': market_price,
            'drawing_price': drawing_price,
            'step': 'quantity'
        })
        
        keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_products")
        await update.message.reply_text(
            "📦 Введите количество продукции (шт):",
            reply_markup=keyboard
        )
        return
    
    # Обработка ввода количества
    elif session['step'] == 'quantity':
        try:
            qty = float(text.replace(',', '.'))
        except ValueError:
            keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_prices")
            await update.message.reply_text(
                "❌ Введите число",
                reply_markup=keyboard
            )
            return
        
        product = session['product']
        output = session['output_per_drawing']
        
        if qty % output != 0:
            keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_prices")
            await update.message.reply_text(
                f"⚠️ Количество должно быть кратно {output}",
                reply_markup=keyboard
            )
            return
        
        drawings_needed = int(qty // output)
        data = load_from_yandex()
        
        materials_dict = collect_materials(
            product['Код'], 1,
            data['nomenclature'],
            data['specifications']
        )
        
        if not materials_dict:
            keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_prices")
            await update.message.reply_text(
                "❌ Нет материалов для этого изделия",
                reply_markup=keyboard
            )
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
        
        await show_materials_page(update, session, edit=False, page=0)
        return
    
    # Обработка пошагового ввода цен материалов
    elif session['step'] == 'price_input_waiting':
        try:
            price = float(text.replace(',', '.'))
        except ValueError:
            current = session.get('current_material', 0)
            materials = session['materials_list']
            m = materials[current]
            
            keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_materials")
            await update.message.reply_text(
                f"❌ Введите число для {m['name']}",
                reply_markup=keyboard
            )
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
    
    # Обработка ввода недостающих цен материалов
    elif session['step'] == 'price_input_missing_waiting':
        try:
            price = float(text.replace(',', '.'))
        except ValueError:
            current = session.get('current_missing_index', 0)
            missing_materials = session.get('missing_materials', [])
            missing = missing_materials[current]
            material = session['materials_list'][missing['index']]
            
            keyboard = get_back_cancel_keyboard(user_id, back_callback="back_to_materials")
            await update.message.reply_text(
                f"❌ Введите число для {material['name']}",
                reply_markup=keyboard
            )
            return
        
        current = session.get('current_missing_index', 0)
        missing_materials = session.get('missing_materials', [])
        
        if current < len(missing_materials):
            missing = missing_materials[current]
            material = session['materials_list'][missing['index']]
            
            material['price'] = price
            material['cost'] = price * material['qty']
            save_material_price(material['name'], price)
            
            session['current_missing_index'] = current + 1
            await process_next_missing_price(update, session)
        return

# ==================== ЗАПУСК ====================
def main():
    init_prices_db()
    
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    
    logger.info("✅ Бот запущен с поддержкой подкатегорий и кнопками навигации")
    app.run_polling()

if __name__ == "__main__":
    main()
