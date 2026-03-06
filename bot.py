import logging
import time
import pandas as pd
import requests
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

# ==================== ХРАНИЛИЩЕ ДАННЫХ ====================
cached_data = None
last_update = 0
sessions = {}

# ==================== ЗАГРУЗКА С GOOGLE ДИСКА ====================
def load_from_yandex():
    """Загружает Excel-файл с Google Диска"""
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
        
        # Читаем Excel
        workbook = pd.ExcelFile(BytesIO(response.content))
        
        # Загружаем листы
        nomenclature = pd.read_excel(workbook, sheet_name='Номенклатура').to_dict('records')
        specifications = pd.read_excel(workbook, sheet_name='Спецификации').to_dict('records')
        
        logger.info(f"Загружено: номенклатура {len(nomenclature)} записей")
        logger.info(f"Спецификации: {len(specifications)} записей")
        
        cached_data = {
            'nomenclature': nomenclature,
            'specifications': specifications
        }
        last_update = current_time
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

# ==================== ПОИСК ИЗДЕЛИЯ ПО КОДУ ====================
def find_product_by_code(code, nomenclature):
    for item in nomenclature:
        item_code = item.get('Код') or item.get('code') or item.get('код')
        if item_code and str(item_code) == str(code):
            return item
    return None

# ==================== РЕКУРСИВНЫЙ СБОР МАТЕРИАЛОВ ====================
def collect_materials(product_code, multiplier, nomenclature, specifications):
    """Рекурсивный сбор материалов с поддержкой разных названий полей"""
    materials = {}
    logger.info(f"Сбор материалов для продукта: {product_code}")
    
    # Покажем пример структуры для отладки
    if specifications:
        logger.info(f"Пример спецификации: {specifications[0]}")
        logger.info(f"Ключи в спецификации: {list(specifications[0].keys())}")
    
    def explode(code, mult):
        logger.info(f"Обрабатываем узел: {code}, множитель: {mult}")
        found = 0
        
        for spec in specifications:
            # Пробуем разные возможные названия полей
            parent = spec.get('Родитель') or spec.get('parent') or spec.get('родитель')
            child = spec.get('Потомок') or spec.get('child') or spec.get('потомок')
            quantity = spec.get('Количество') or spec.get('quantity') or spec.get('количество', 0)
            
            if parent and str(parent) == str(code):
                found += 1
                logger.info(f"  Найдена спецификация: {parent} -> {child}, кол-во: {quantity}")
                
                # Ищем элемент в номенклатуре
                found_item = None
                for item in nomenclature:
                    item_code = item.get('Код') or item.get('code') or item.get('код')
                    if item_code and str(item_code) == str(child):
                        found_item = item
                        break
                
                if found_item:
                    item_type = str(found_item.get('Тип') or found_item.get('type') or found_item.get('тип', '')).lower()
                    item_name = found_item.get('Наименование') or found_item.get('name') or found_item.get('наименование', 'Неизвестно')
                    
                    if 'материал' in item_type:
                        if child not in materials:
                            materials[child] = {
                                'name': item_name,
                                'baseQty': 0
                            }
                        qty = float(quantity) * mult
                        materials[child]['baseQty'] += qty
                        logger.info(f"    Материал: {item_name}, добавляем {qty}")
                    elif 'узел' in item_type:
                        logger.info(f"    Узел: {item_name}, углубляемся")
                        explode(child, mult * float(quantity))
                    else:
                        logger.info(f"    Пропускаем (тип: {item_type})")
                else:
                    logger.info(f"  Элемент с кодом {child} не найден в номенклатуре")
        
        if found == 0:
            logger.info(f"  Для {code} спецификаций не найдено")
    
    explode(product_code, multiplier)
    logger.info(f"Собрано материалов: {len(materials)}")
    return materials

# ==================== РАСЧЕТ МАТЕРИАЛОВ ====================
def calculate_materials(materials, qty, drawings_needed, efficiency, material_prices):
    materials_list = []
    total_cost = 0
    
    i = 1
    for m in materials.values():
        raw = (m['baseQty'] / 1.5) * (efficiency / 100)
        rounded = (raw * 10 // 1 + 1) / 10 if raw * 10 % 1 > 0 else raw
        final_qty = rounded * drawings_needed
        
        price = material_prices.get(m['name'], 0)
        cost = final_qty * price
        total_cost += cost
        
        materials_list.append({
            'name': m['name'],
            'qty': final_qty,
            'price': price,
            'cost': cost,
            'number': i
        })
        i += 1
    
    return materials_list, total_cost

# ==================== ФОРМАТИРОВАНИЕ ====================
def format_number(num):
    return f"{num:,.2f}".replace(",", " ")

def format_materials_for_input(materials_list):
    """Формирует список материалов для запроса цен"""
    result = "📦 *Материалы*\n\nВведите цены через пробел в том же порядке:\n\n"
    for m in materials_list:
        result += f"{m['number']}. {m['name']} — нужно {format_number(m['qty'])} шт\n"
    return result

def format_results(product_name, qty, efficiency, tax_rate, materials_list, result):
    """Формирует финальный отчет"""
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
    """Показывает страницу со списком изделий"""
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
    
    # Создаем клавиатуру для навигации
    keyboard = []
    nav_row = []
    
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Назад", callback_data="prev_page"))
    if end_idx < len(products):
        nav_row.append(InlineKeyboardButton("Вперед ▶️", callback_data="next_page"))
    
    if nav_row:
        keyboard.append(nav_row)
    
    # Кнопка возврата к категориям
    keyboard.append([InlineKeyboardButton("🔙 К категориям", callback_data="back_to_categories")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    
    if edit:
        await update_or_query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await update_or_query.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

# ==================== КОМАНДЫ ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update):
        return
    
    user_id = update.effective_user.id
    sessions[user_id] = {'step': 'categories'}
    
    data = load_from_yandex()
    
    if not data['nomenclature']:
        await update.message.reply_text("❌ Ошибка загрузки данных")
        return
    
    # Получаем и очищаем категории
    raw_categories = list(set(
        item.get('Категории', '') for item in data['nomenclature'] 
        if item.get('Категории')
    ))
    
    clean_categories = []
    for cat in raw_categories:
        if cat and str(cat).strip():
            clean_cat = str(cat).strip()
            if clean_cat:
                clean_categories.append(clean_cat)
    
    if not clean_categories:
        await update.message.reply_text("❌ В базе нет категорий")
        return
    
    # Создаем клавиатуру
    keyboard = []
    for cat in clean_categories[:10]:
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
    
    if data == "cancel":
        sessions.pop(user_id, None)
        await query.edit_message_text("❌ Расчет отменен")
        return
    
    if data in ["prev_page", "next_page"]:
        session = sessions.get(user_id)
        if session and session.get('step') == 'product_selection':
            if data == "prev_page":
                session['product_page'] = max(0, session.get('product_page', 0) - 1)
            else:
                session['product_page'] = session.get('product_page', 0) + 1
            
            await show_products_page(query, session, edit=True)
        return
    
    if data == "back_to_categories":
        sessions.pop(user_id, None)
        data = load_from_yandex()
        
        raw_categories = list(set(
            item.get('Категории', '') for item in data['nomenclature'] 
            if item.get('Категории')
        ))
        
        clean_categories = []
        for cat in raw_categories:
            if cat and str(cat).strip():
                clean_cat = str(cat).strip()
                if clean_cat:
                    clean_categories.append(clean_cat)
        
        keyboard = []
        for cat in clean_categories[:10]:
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
        sessions[user_id] = {
            'step': 'parameters',
            'category': category
        }
        
        keyboard = [[InlineKeyboardButton("🔙 К категориям", callback_data="back_to_categories")]]
        
        await query.edit_message_text(
            f"📊 *Параметры расчета*\nКатегория: {category}\n\n"
            f"Введите через пробел:\n"
            f"`Эффективность (%) Налог (%)`\n\n"
            f"Пример: `150 20`",
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
    
    text = update.message.text
    logger.info(f"Текст от {user_id}: {text}, шаг: {session.get('step')}")
    
    if session['step'] == 'parameters':
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text(
                "❌ Введите через пробел: Эффективность Налог\nПример: 150 20"
            )
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
                products.append({
                    'code': item['Код'],
                    'name': item['Наименование']
                })
        
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
    
    elif session['step'] == 'product_selection':
        try:
            idx = int(text) - 1
            if idx < 0 or idx >= len(session['products']):
                raise ValueError
            selected = session['products'][idx]
        except:
            await update.message.reply_text(
                f"❌ Введите число от 1 до {len(session['products'])}"
            )
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
        
        # Получаем кратность с проверкой
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
        
        await update.message.reply_text(
            f"✅ Выбрано: *{product['Наименование']}*\n"
            f"Кратность: {multiplicity}\n\n"
            f"💰 Введите через пробел:\n"
            f"`Рыночная цена Стоимость чертежа`\n\n"
            f"Пример: `3200000 6900000`",
            parse_mode='Markdown'
        )
        return
    
    elif session['step'] == 'prices':
        parts = text.split()
        if len(parts) < 2:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data="back_to_products")]]
            await update.message.reply_text(
                "❌ Введите две цены через пробел\nПример: 3200000 6900000",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        try:
            market_price = float(parts[0])
            drawing_price = float(parts[1])
        except ValueError:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data="back_to_products")]]
            await update.message.reply_text(
                "❌ Введите числа",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        session.update({
            'market_price': market_price,
            'drawing_price': drawing_price,
            'step': 'quantity'
        })
        
        await update.message.reply_text(
            f"📦 Введите количество продукции (шт):"
        )
        return
    
    elif session['step'] == 'quantity':
        try:
            qty = float(text)
        except ValueError:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data="back_to_products")]]
            await update.message.reply_text(
                "❌ Введите число",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        product = session['product']
        output = session['output_per_drawing']
        
        if qty % output != 0:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data="back_to_products")]]
            await update.message.reply_text(
                f"⚠️ Количество должно быть кратно {output}",
                reply_markup=InlineKeyboardMarkup(keyboard)
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
        
        session.update({
            'step': 'material_prices',
            'qty': qty,
            'drawings_needed': drawings_needed,
            'materials_list': materials_list
        })
        
        await update.message.reply_text(
            format_materials_for_input(materials_list),
            parse_mode='Markdown'
        )
        return
    
    elif session['step'] == 'material_prices':
        parts = text.split()
        if len(parts) < len(session['materials_list']):
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data="back_to_products")]]
            await update.message.reply_text(
                f"❌ Введите {len(session['materials_list'])} цен через пробел",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        try:
            prices = [float(p) for p in parts[:len(session['materials_list'])]]
        except ValueError:
            keyboard = [[InlineKeyboardButton("🔙 К выбору изделия", callback_data="back_to_products")]]
            await update.message.reply_text(
                "❌ Введите числа",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        for i, m in enumerate(session['materials_list']):
            m['price'] = prices[i]
            m['cost'] = m['qty'] * prices[i]
        
        material_cost = sum(m['qty'] * m['price'] for m in session['materials_list'])
        
        price_str = str(session['product'].get('Цена производства', '0'))
        price_clean = price_str.replace(' ISK', '').replace(' ', '')
        try:
            prod_cost_per_unit = float(price_clean) if price_clean else 0
        except:
            prod_cost_per_unit = 0
        
        prod_cost = prod_cost_per_unit * session['drawings_needed']
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
        return

# ==================== ЗАПУСК ====================
def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    logger.info("Бот запущен")
    app.run_polling()

if __name__ == "__main__":
    main()
