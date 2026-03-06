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
        
        # Загружаем листы с правильными названиями
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

# ==================== ПОИСК ИЗДЕЛИЯ ====================
def find_product(product_name, nomenclature):
    for item in nomenclature:
        if item.get('Наименование') and item['Наименование'].lower() == product_name.lower():
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
def calculate_materials(materials, qty, drawings_needed, efficiency):
    materials_list = []
    total_cost = 0
    
    for m in materials.values():
        raw = (m['baseQty'] / 1.5) * (efficiency / 100)
        rounded = (raw * 10 // 1 + 1) / 10 if raw * 10 % 1 > 0 else raw
        final_qty = rounded * drawings_needed
        price = 100  # ВРЕМЕННО
        cost = final_qty * price
        total_cost += cost
        
        materials_list.append({
            'name': m['name'],
            'qty': final_qty,
            'price': price,
            'cost': cost
        })
    
    return materials_list, total_cost

# ==================== ФОРМАТИРОВАНИЕ ====================
def format_number(num):
    return f"{num:,.2f}".replace(",", " ")

def format_materials(materials):
    result = ""
    for i, m in enumerate(materials, 1):
        result += f"{i}. {m['name']}: {format_number(m['qty'])} шт × {format_number(m['price'])}₽ = {format_number(m['cost'])}₽\n"
    return result

def format_money_block(data):
    result = f"💰 ДЕНЬГИ\n\n"
    result += f"ИТОГО за {data['qty']} шт:\n"
    result += f"Материалы: {format_number(data['materialCost'])}₽\n"
    result += f"Производство: {format_number(data['prodCost'])}₽\n"
    result += f"Чертежи: {format_number(data['drawingCost'])}₽\n"
    result += f"Себестоимость: {format_number(data['totalCost'])}₽\n"
    result += f"Выручка: {format_number(data['revenue'])}₽\n"
    result += f"Прибыль до налога: {format_number(data['profitBeforeTax'])}₽\n"
    result += f"Налог ({data['taxRate']}%): {format_number(data['tax'])}₽\n"
    result += f"Прибыль после налога: {format_number(data['profitAfterTax'])}₽\n"
    
    if data['qty'] > 0:
        per_unit = data['totalCost'] / data['qty']
        per_unit_profit = data['profitAfterTax'] / data['qty']
        result += f"\nНА 1 ШТУКУ:\n"
        result += f"Себестоимость: {format_number(per_unit)}₽\n"
        result += f"Прибыль: {format_number(per_unit_profit)}₽\n"
    
    return result

# ==================== КОМАНДЫ ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update):
        return
    
    user_id = update.effective_user.id
    if user_id in sessions:
        del sessions[user_id]
    
    data = load_from_yandex()
    
    if not data['nomenclature']:
        await update.message.reply_text("❌ Ошибка загрузки данных")
        return
    
    # ИСПРАВЛЕНО: 'Категории' вместо 'Категория'
    categories = list(set(
        item.get('Категории', '') for item in data['nomenclature'] 
        if item.get('Категории')
    ))
    
    if not categories:
        await update.message.reply_text("❌ Нет категорий в базе")
        return
    
    keyboard = []
    for cat in categories[:10]:
        keyboard.append([InlineKeyboardButton(cat, callback_data=f"cat_{cat}")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    
    await update.message.reply_text(
        "👋 Производственный калькулятор\n\nВыберите категорию:",
        reply_markup=InlineKeyboardMarkup(keyboard)
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
        sessions[user_id] = {'step': 'parameters', 'category': category}
        keyboard = [
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_categories")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel")]
        ]
        await query.edit_message_text(
            f"📊 Параметры расчета\nКатегория: {category}\n\nВведите через пробел:\nЭффективность (%) Налог (%)\nПример: 150 20",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
    
    if data == "back_to_categories":
        sessions.pop(user_id, None)
        data = load_from_yandex()
        categories = list(set(item.get('Категории', '') for item in data['nomenclature'] if item.get('Категории')))
        keyboard = []
        for cat in categories[:10]:
            keyboard.append([InlineKeyboardButton(cat, callback_data=f"cat_{cat}")])
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        await query.edit_message_text(
            "👋 Производственный калькулятор\n\nВыберите категорию:",
            reply_markup=InlineKeyboardMarkup(keyboard)
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
    
    if session['step'] == 'parameters':
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text("❌ Введите: Эффективность Налог\nПример: 150 20")
            return
        
        try:
            efficiency = float(parts[0])
            tax = float(parts[1])
        except:
            await update.message.reply_text("❌ Введите числа")
            return
        
        session.update({'step': 'product_selection', 'efficiency': efficiency, 'tax': tax})
        
        data = load_from_yandex()
        products = [item['Наименование'] for item in data['nomenclature'] 
                   if item.get('Тип') in ['изделие', 'узел']]
        products.sort()
        
        await update.message.reply_text(
            f"✅ Параметры сохранены\nЭффективность: {efficiency:.0f}%\nНалог: {tax:.0f}%\n\n"
            f"📋 Введите название изделия или узла:"
        )
        return
    
    elif session['step'] == 'product_selection':
        data = load_from_yandex()
        product = find_product(text, data['nomenclature'])
        
        if not product:
            await update.message.reply_text(f"❌ Изделие не найдено")
            return
        
        # ИСПРАВЛЕНО: 'Кратность' вместо 'Выход_с_чертежа'
        session.update({
            'step': 'quantities',
            'product': product,
            'output_per_drawing': int(product.get('Кратность', 1))
        })
        
        await update.message.reply_text(
            f"✅ Выбрано: {product['Наименование']}\n"
            f"Кратность: {product.get('Кратность', 1)}\n\n"
            f"💰 Введите через пробел:\nРыночная цена Стоимость чертежа\nПример: 3200000 6900000"
        )
        return
    
    elif session['step'] == 'quantities':
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text("❌ Введите две цены")
            return
        
        try:
            market_price = float(parts[0])
            drawing_price = float(parts[1])
        except:
            await update.message.reply_text("❌ Введите числа")
            return
        
        session.update({'market_price': market_price, 'drawing_price': drawing_price})
        await update.message.reply_text("📦 Введите количество продукции (шт):")
        session['step'] = 'final_calculation'
        return
    
    elif session['step'] == 'final_calculation':
        try:
            qty = float(text)
        except:
            await update.message.reply_text("❌ Введите число")
            return
        
        product = session['product']
        output = session['output_per_drawing']
        
        if qty % output != 0:
            await update.message.reply_text(f"⚠️ Количество должно быть кратно {output}")
            return
        
        drawings_needed = int(qty // output)
        data = load_from_yandex()
        
        # ИСПРАВЛЕНО: 'Родитель' и 'Потомок' вместо 'Родитель_код' и 'Потомок_код'
        materials_dict = collect_materials(product['Код'], 1, data['nomenclature'], data['specifications'])
        materials_list, material_cost = calculate_materials(materials_dict, qty, drawings_needed, session['efficiency'])
        
        # ИСПРАВЛЕНО: 'Цена производства' вместо 'Цена производства ISK'
        prod_cost = float(str(product.get('Цена производства', '0')).replace(' ISK', '').replace(' ', '')) * drawings_needed
        drawing_cost = session['drawing_price'] * drawings_needed
        total = material_cost + prod_cost + drawing_cost
        revenue = session['market_price'] * qty
        profit_before = revenue - total
        tax = profit_before * session['tax'] / 100 if profit_before > 0 else 0
        profit_after = profit_before - tax
        
        result = {
            'qty': qty,
            'materialCost': material_cost,
            'prodCost': prod_cost,
            'drawingCost': drawing_cost,
            'totalCost': total,
            'revenue': revenue,
            'profitBeforeTax': profit_before,
            'tax': tax,
            'profitAfterTax': profit_after,
            'taxRate': session['tax']
        }
        
        await update.message.reply_text(
            f"📊 РЕЗУЛЬТАТЫ РАСЧЕТА\n\n"
            f"Изделие: {product['Наименование']}\n"
            f"Количество: {qty:.0f} шт\n"
            f"Эффективность: {session['efficiency']:.0f}%\n"
            f"Налог: {session['tax']:.0f}%\n\n"
            f"Материалы:\n{format_materials(materials_list)}\n"
            f"{format_money_block(result)}"
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
