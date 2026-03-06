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
        if item.get('Код') == code:
            return item
    return None

# ==================== РЕКУРСИВНЫЙ СБОР МАТЕРИАЛОВ ====================
def collect_materials(product_code, multiplier, nomenclature, specifications):
    materials = {}
    
    def explode(code, mult):
        for spec in specifications:
            if str(spec.get('Родитель')) == str(code):
                for item in nomenclature:
                    if str(item.get('Код')) == str(spec.get('Потомок')):
                        if item.get('Тип') == 'материал':
                            if item['Код'] not in materials:
                                materials[item['Код']] = {
                                    'name': item['Наименование'],
                                    'baseQty': 0
                                }
                            qty = float(spec.get('Количество', 0)) * mult
                            materials[item['Код']]['baseQty'] += qty
                        elif item.get('Тип') == 'узел':
                            explode(item['Код'], mult * float(spec.get('Количество', 0)))
    
    explode(product_code, multiplier)
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
    
    if data.startswith("cat_"):
        category = data[4:]
        sessions[user_id] = {
            'step': 'parameters',
            'category': category
        }
        
        await query.edit_message_text(
            f"📊 *Параметры расчета*\nКатегория: {category}\n\n"
            f"Введите через пробел:\n"
            f"`Эффективность (%) Налог (%)`\n\n"
            f"Пример: `150 20`",
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
    
    # Шаг 1: Ввод эффективности и налога
    if session['step'] == 'parameters':
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text(
                "❌ Введите через пробел: Эффективность Налог\n"
                "Пример: 150 20"
            )
            return
        
        try:
            efficiency = float(parts[0])
            tax = float(parts[1])
        except ValueError:
            await update.message.reply_text("❌ Введите числа")
            return
        
        data = load_from_yandex()
        
        # Получаем изделия для выбранной категории
        products = []
        for item in data['nomenclature']:
            if item.get('Категории') == session['category'] and item.get('Тип') in ['изделие', 'узел']:
                products.append({
                    'code': item['Код'],
                    'name': item['Наименование']
                })
        
        if not products:
            await update.message.reply_text("❌ Нет изделий в этой категории")
            return
        
        # Сохраняем в сессию
        session.update({
            'step': 'product_selection',
            'efficiency': efficiency,
            'tax': tax,
            'products': products
        })
        
        # Показываем список изделий
        products_list = "\n".join([f"{i+1}. {p['name']}" for i, p in enumerate(products[:20])])
        if len(products) > 20:
            products_list += f"\n... и еще {len(products) - 20}"
        
        await update.message.reply_text(
            f"✅ Параметры сохранены\n"
            f"Эффективность: {efficiency:.0f}%\n"
            f"Налог: {tax:.0f}%\n\n"
            f"📋 *Доступные изделия:*\n{products_list}\n\n"
            f"Введите **номер** изделия из списка:",
            parse_mode='Markdown'
        )
        return
    
    # Шаг 2: Выбор изделия по номеру
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
        
        # Получаем полные данные изделия
        data = load_from_yandex()
        product = None
        for item in data['nomenclature']:
            if item['Код'] == selected['code']:
                product = item
                break
        
        if not product:
            await update.message.reply_text("❌ Ошибка получения данных")
            return
        
        session.update({
            'step': 'prices',
            'product': product,
            'output_per_drawing': int(product.get('Кратность', 1))
        })
        
        await update.message.reply_text(
            f"✅ Выбрано: *{product['Наименование']}*\n"
            f"Кратность: {product.get('Кратность', 1)}\n\n"
            f"💰 Введите через пробел:\n"
            f"`Рыночная цена Стоимость чертежа`\n\n"
            f"Пример: `3200000 6900000`",
            parse_mode='Markdown'
        )
        return
    
    # Шаг 3: Ввод цен
    elif session['step'] == 'prices':
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text(
                "❌ Введите две цены через пробел\n"
                "Пример: 3200000 6900000"
            )
            return
        
        try:
            market_price = float(parts[0])
            drawing_price = float(parts[1])
        except ValueError:
            await update.message.reply_text("❌ Введите числа")
            return
        
        # Запрашиваем количество
        session.update({
            'market_price': market_price,
            'drawing_price': drawing_price,
            'step': 'quantity'
        })
        
        await update.message.reply_text(
            f"📦 Введите количество продукции (шт):"
        )
        return
    
    # Шаг 4: Ввод количества и расчет материалов
    elif session['step'] == 'quantity':
        try:
            qty = float(text)
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
        
        # Собираем материалы
        materials_dict = collect_materials(
            product['Код'], 1, 
            data['nomenclature'], 
            data['specifications']
        )
        
        if not materials_dict:
            await update.message.reply_text("❌ Нет материалов для этого изделия")
            return
        
        # Рассчитываем количества (без цен)
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
        
        # Сохраняем в сессию
        session.update({
            'step': 'material_prices',
            'qty': qty,
            'drawings_needed': drawings_needed,
            'materials_list': materials_list
        })
        
        # Запрашиваем цены на материалы
        await update.message.reply_text(
            format_materials_for_input(materials_list),
            parse_mode='Markdown'
        )
        return
    
    # Шаг 5: Ввод цен на материалы
    elif session['step'] == 'material_prices':
        parts = text.split()
        if len(parts) < len(session['materials_list']):
            await update.message.reply_text(
                f"❌ Введите {len(session['materials_list'])} цен через пробел"
            )
            return
        
        try:
            prices = [float(p) for p in parts[:len(session['materials_list'])]]
        except ValueError:
            await update.message.reply_text("❌ Введите числа")
            return
        
        # Создаем словарь цен
        material_prices = {}
        for i, m in enumerate(session['materials_list']):
            material_prices[m['name']] = prices[i]
            m['price'] = prices[i]
            m['cost'] = m['qty'] * prices[i]
        
        # Рассчитываем итоги
        material_cost = sum(m['qty'] * m['price'] for m in session['materials_list'])
        
        # Цена производства
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
        
        # Отправляем результат
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
        
        # Очищаем сессию
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
