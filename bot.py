import logging
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from config import TOKEN, GROUP_ID, TOPIC_ID
import database as db

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== ХРАНИЛИЩЕ СЕССИЙ ====================
sessions = {}

# ==================== ПРОВЕРКА ДОСТУПА ====================
async def check_access(update: Update) -> bool:
    """Проверяет, что сообщение из нужной группы и темы"""
    if not GROUP_ID or not TOPIC_ID:
        return True
    
    chat_id = update.effective_chat.id
    topic_id = update.message.message_thread_id if update.message else None
    
    if chat_id == GROUP_ID and topic_id == TOPIC_ID:
        return True
    
    logger.warning(f"Доступ запрещен: chat={chat_id}, topic={topic_id}")
    return False

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def format_number(num):
    """Форматирование числа с разделителями"""
    return f"{num:,.2f}".replace(",", " ")

def calculate_materials_with_efficiency(materials, qty, efficiency):
    """Расчет количества материалов с учетом эффективности"""
    result = []
    total_cost = 0
    
    for m in materials:
        # Формула: (базовое_количество / 1.5) * (эффективность / 100)
        raw = (m['quantity'] / 1.5) * (efficiency / 100)
        # Округление вверх до 1 знака
        rounded = (raw * 10 // 1 + 1) / 10 if raw * 10 % 1 > 0 else raw
        final_qty = rounded * qty
        
        # Пока цены тестовые (потом можно брать из базы)
        price = 100
        cost = final_qty * price
        total_cost += cost
        
        result.append({
            'name': m['name'],
            'quantity': final_qty,
            'price': price,
            'cost': cost
        })
    
    return result, total_cost

def format_materials(materials):
    """Форматирование списка материалов"""
    result = ""
    for i, m in enumerate(materials, 1):
        result += f"{i}. {m['name']}: {format_number(m['quantity'])} шт"
        if m['price'] > 0:
            result += f" × {format_number(m['price'])}₽ = {format_number(m['cost'])}₽\n"
        else:
            result += " (цена не указана)\n"
    return result

def format_money_block(data):
    """Форматирование блока Деньги"""
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
        per_unit_material = data['materialCost'] / data['qty']
        per_unit_total = data['totalCost'] / data['qty']
        per_unit_profit = data['profitAfterTax'] / data['qty']
        
        result += f"\nНА 1 ШТУКУ:\n"
        result += f"Материалы: {format_number(per_unit_material)}₽\n"
        result += f"Себестоимость: {format_number(per_unit_total)}₽\n"
        result += f"Прибыль: {format_number(per_unit_profit)}₽\n"
    
    return result

# ==================== ОБРАБОТЧИКИ КОМАНД ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка команды /start"""
    logger.info(f"📨 /start от {update.effective_user.id}")
    
    if not await check_access(update):
        return
    
    user_id = update.effective_user.id
    if user_id in sessions:
        del sessions[user_id]
    
    # Инициализируем базу данных при первом запуске
    db.init_database()
    
    categories = db.get_all_categories()
    
    if not categories:
        await update.message.reply_text("❌ Нет доступных категорий в базе данных")
        return
    
    # Создаем клавиатуру с категориями
    keyboard = []
    for cat in categories[:10]:  # Ограничим 10 категориями
        keyboard.append([InlineKeyboardButton(cat, callback_data=f"cat_{cat}")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    
    await update.message.reply_text(
        "👋 Производственный калькулятор\n\n"
        "Выберите категорию для расчета:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий на кнопки"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    data = query.data
    
    if data == "cancel":
        if user_id in sessions:
            del sessions[user_id]
        await query.edit_message_text("❌ Расчет отменен. Используйте /start для нового расчета.")
        return
    
    if data.startswith("cat_"):
        category = data[4:]
        sessions[user_id] = {
            'step': 'parameters',
            'category': category
        }
        
        keyboard = [
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_categories")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel")]
        ]
        
        await query.edit_message_text(
            f"📊 Параметры расчета\n"
            f"Категория: {category}\n\n"
            f"Введите через пробел:\n"
            f"Эффективность (%) Налог (%)\n\n"
            f"Пример: 150 20",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
    
    if data == "back_to_categories":
        if user_id in sessions:
            del sessions[user_id]
        
        categories = db.get_all_categories()
        keyboard = []
        for cat in categories[:10]:
            keyboard.append([InlineKeyboardButton(cat, callback_data=f"cat_{cat}")])
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        
        await query.edit_message_text(
            "👋 Производственный калькулятор\n\n"
            "Выберите категорию:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений"""
    if not await check_access(update):
        return
    
    user_id = update.effective_user.id
    session = sessions.get(user_id)
    
    if not session:
        await update.message.reply_text("Используйте /start для начала расчета")
        return
    
    text = update.message.text
    
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
        
        session.update({
            'step': 'product_selection',
            'efficiency': efficiency,
            'tax': tax
        })
        
        # Получаем изделия для выбранной категории
        products = db.get_products_by_category(session['category'])
        
        if not products:
            await update.message.reply_text("❌ Нет изделий в этой категории")
            return
        
        products_list = "\n".join([f"• {p['name']}" for p in products[:20]])
        if len(products) > 20:
            products_list += f"\n... и еще {len(products) - 20}"
        
        await update.message.reply_text(
            f"✅ Параметры сохранены:\n"
            f"Эффективность: {efficiency:.0f}%\n"
            f"Налог: {tax:.0f}%\n\n"
            f"📋 Доступные изделия:\n{products_list}\n\n"
            f"Введите название изделия или узла:"
        )
        return
    
    elif session['step'] == 'product_selection':
        product = db.find_product_by_name(text)
        
        if not product:
            await update.message.reply_text(
                f"❌ Изделие '{text}' не найдено. Попробуйте еще раз."
            )
            return
        
        # Получаем кратность из базы
        try:
            output_per_drawing = int(product.get('multiplicity', 1))
        except:
            output_per_drawing = 1
        
        session.update({
            'step': 'quantities',
            'product': product,
            'output_per_drawing': output_per_drawing
        })
        
        await update.message.reply_text(
            f"✅ Выбрано: {product['name']}\n"
            f"Кратность: {output_per_drawing}\n\n"
            f"💰 Введите через пробел:\n"
            f"Рыночная цена Стоимость чертежа изделия\n\n"
            f"Пример: 3200000 6900000"
        )
        return
    
    elif session['step'] == 'quantities':
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text(
                "❌ Введите через пробел: Рыночная цена Стоимость чертежа\n"
                "Пример: 3200000 6900000"
            )
            return
        
        try:
            market_price = float(parts[0])
            drawing_price = float(parts[1])
        except ValueError:
            await update.message.reply_text("❌ Введите числа")
            return
        
        session.update({
            'market_price': market_price,
            'drawing_price': drawing_price
        })
        
        await update.message.reply_text(
            f"📦 Введите количество продукции (шт):"
        )
        
        session['step'] = 'final_calculation'
        return
    
    elif session['step'] == 'final_calculation':
        try:
            qty = float(text)
        except ValueError:
            await update.message.reply_text("❌ Введите число")
            return
        
        # Проверка кратности
        output_per_drawing = session.get('output_per_drawing', 1)
        if qty % output_per_drawing != 0:
            await update.message.reply_text(
                f"⚠️ Количество должно быть кратно {output_per_drawing}"
            )
            return
        
        drawings_needed = int(qty // output_per_drawing)
        
        # Получаем материалы из базы
        product_code = session['product']['code']
        materials = db.get_materials_for_product(product_code)
        
        if not materials:
            await update.message.reply_text("❌ Нет материалов для этого изделия")
            return
        
        # Рассчитываем материалы с эффективностью
        materials_list, material_cost = calculate_materials_with_efficiency(
            materials, qty, session['efficiency']
        )
        
        # Получаем цену производства из базы
        try:
            price_str = session['product'].get('price', '0')
            # Очищаем строку от " ISK" и пробелов
            price_clean = price_str.replace(' ISK', '').replace(' ', '')
            prod_cost_per_unit = float(price_clean) if price_clean else 0
        except:
            prod_cost_per_unit = 0
        
        prod_cost = prod_cost_per_unit * drawings_needed
        drawing_cost = session['drawing_price'] * drawings_needed
        total_cost = material_cost + prod_cost + drawing_cost
        revenue = session['market_price'] * qty
        profit_before_tax = revenue - total_cost
        tax = profit_before_tax * session['tax'] / 100 if profit_before_tax > 0 else 0
        profit_after_tax = profit_before_tax - tax
        
        result = {
            'qty': qty,
            'materialCost': material_cost,
            'prodCost': prod_cost,
            'drawingCost': drawing_cost,
            'totalCost': total_cost,
            'revenue': revenue,
            'profitBeforeTax': profit_before_tax,
            'tax': tax,
            'profitAfterTax': profit_after_tax,
            'taxRate': session['tax']
        }
        
        materials_text = format_materials(materials_list)
        money_text = format_money_block(result)
        
        await update.message.reply_text(
            f"📊 РЕЗУЛЬТАТЫ РАСЧЕТА\n\n"
            f"Изделие: {session['product']['name']}\n"
            f"Количество: {qty:.0f} шт\n"
            f"Эффективность: {session['efficiency']:.0f}%\n"
            f"Налог: {session['tax']:.0f}%\n\n"
            f"Материалы:\n{materials_text}\n"
            f"{money_text}"
        )
        
        # Очищаем сессию
        del sessions[user_id]
        return

# ==================== ЗАПУСК БОТА ====================
def main():
    """Точка входа для запуска бота"""
    try:
        if not TOKEN:
            logger.error("TOKEN не задан")
            return
        
        logger.info(f"Запуск бота...")
        logger.info(f"Группа: {GROUP_ID}, Тема: {TOPIC_ID}")
        
        # Проверяем подключение к базе при старте
        try:
            db.init_database()
            categories = db.get_all_categories()
            logger.info(f"✅ База данных подключена. Категории: {len(categories)}")
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД: {e}")
        
        app = Application.builder().token(TOKEN).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CallbackQueryHandler(button_handler))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
        
        logger.info("Бот запущен и ожидает сообщения...")
        app.run_polling()
        
    except Exception as e:
        logger.exception(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    main()
