import logging
import json
import time
import pandas as pd
import requests
from io import BytesIO
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# Импортируем настройки из config.py
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
sessions = {}  # Хранилище сессий пользователей

# ==================== ЗАГРУЗКА ИЗ ЯНДЕКС ТАБЛИЦЫ ====================
def load_from_yandex():
    """Загружает данные из Яндекс Таблицы с подробной диагностикой"""
    global cached_data, last_update
    
    current_time = time.time()
    if cached_data and (current_time - last_update) < CACHE_TTL:
        logger.info("✅ Используем кэшированные данные")
        return cached_data
    
    logger.info("=" * 50)
    logger.info("НАЧАЛО ЗАГРУЗКИ ИЗ ЯНДЕКС ТАБЛИЦЫ")
    logger.info(f"URL: {YANDEX_TABLE_URL}")
    
    if not YANDEX_TABLE_URL:
        logger.error("❌ YANDEX_TABLE_URL пустой")
        return {'nomenclature': [], 'specifications': []}
    
    try:
        # Шаг 1: Запрос к серверу
        logger.info("Шаг 1: Отправка HTTP запроса...")
        response = requests.get(YANDEX_TABLE_URL, timeout=30)
        logger.info(f"Шаг 2: Статус ответа: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"❌ Ошибка HTTP: {response.status_code}")
            return {'nomenclature': [], 'specifications': []}
        
        # Шаг 3: Размер ответа
        content_length = len(response.content)
        logger.info(f"Шаг 3: Размер ответа: {content_length} байт")
        
        if content_length < 100:
            logger.warning(f"⚠️ Ответ подозрительно маленький: {content_length} байт")
            logger.info(f"Содержимое: {response.content[:200]}")
        
        # Шаг 4: Чтение CSV
        logger.info("Шаг 4: Попытка прочитать CSV...")
        df = pd.read_csv(BytesIO(response.content))
        logger.info(f"✅ CSV прочитан успешно!")
        logger.info(f"Колонки в CSV: {list(df.columns)}")
        logger.info(f"Количество строк: {len(df)}")
        
        # Шаг 5: Здесь должна быть ваша логика преобразования
        # ПОКА ИСПОЛЬЗУЕМ ТЕСТОВЫЕ ДАННЫЕ
        logger.info("Шаг 5: Использую тестовые данные (временное решение)")
        
        nomenclature = [
            {'Код': 'Изд001', 'Наименование': 'Балка', 'Тип': 'Изделие', 
             'Фикс_производство': 500000, 'Выход_с_чертежа': 10, 'Категория': 'Сооружения'},
            {'Код': 'Изд002', 'Наименование': 'Каркас', 'Тип': 'Узел', 
             'Фикс_производство': 200000, 'Выход_с_чертежа': 5, 'Категория': 'Сооружения'},
            {'Код': 'Мат001', 'Наименование': 'Болт М10', 'Тип': 'Материал', 
             'Фикс_производство': 0, 'Выход_с_чертежа': 1, 'Категория': 'Такелаж'},
        ]
        
        specifications = [
            {'Родитель_код': 'Изд001', 'Потомок_код': 'Изд002', 'Количество': 1},
            {'Родитель_код': 'Изд001', 'Потомок_код': 'Мат001', 'Количество': 4},
            {'Родитель_код': 'Изд002', 'Потомок_код': 'Мат001', 'Количество': 2},
        ]
        
        cached_data = {
            'nomenclature': nomenclature,
            'specifications': specifications
        }
        last_update = current_time
        
        logger.info("✅ Данные успешно загружены (тестовые)")
        logger.info(f"Номенклатура: {len(nomenclature)} записей")
        logger.info(f"Спецификации: {len(specifications)} записей")
        logger.info("=" * 50)
        
        return cached_data
        
    except requests.exceptions.Timeout:
        logger.error("❌ Таймаут при запросе к Яндекс Таблице")
    except requests.exceptions.ConnectionError:
        logger.error("❌ Ошибка соединения с Яндекс Таблицей")
    except pd.errors.EmptyDataError:
        logger.error("❌ CSV файл пустой")
    except Exception as e:
        logger.error(f"❌ Неожиданная ошибка: {type(e).__name__}: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.error("❌ Загрузка не удалась, возвращаем пустые данные")
    logger.info("=" * 50)
    return {'nomenclature': [], 'specifications': []}
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
def find_product(product_name, nomenclature):
    """Поиск изделия по названию"""
    for item in nomenclature:
        if item.get('Наименование') and item['Наименование'].lower() == product_name.lower():
            return item
    return None

def collect_materials(product_code, multiplier, nomenclature, specifications):
    """Рекурсивный сбор материалов"""
    materials = {}
    
    def explode(code, mult):
        for spec in specifications:
            if str(spec.get('Родитель_код')) == str(code):
                for item in nomenclature:
                    if str(item.get('Код')) == str(spec.get('Потомок_код')):
                        if item.get('Тип') == 'Материал':
                            if item['Код'] not in materials:
                                materials[item['Код']] = {
                                    'name': item['Наименование'],
                                    'baseQty': 0
                                }
                            qty = float(spec.get('Количество', 0)) * mult
                            materials[item['Код']]['baseQty'] += qty
                        elif item.get('Тип') == 'Узел':
                            explode(item['Код'], mult * float(spec.get('Количество', 0)))
    
    explode(product_code, multiplier)
    return materials

def calculate_materials(materials, qty, drawings_needed, efficiency):
    """Расчет количества материалов с эффективностью"""
    materials_list = []
    
    for m in materials.values():
        # Формула: (baseQty / 1.5) * (efficiency / 100)
        raw = (m['baseQty'] / 1.5) * (efficiency / 100)
        # Округление вверх до 1 знака
        rounded = (raw * 10 // 1 + 1) / 10 if raw * 10 % 1 > 0 else raw
        final_qty = rounded * drawings_needed
        
        materials_list.append({
            'name': m['name'],
            'qty': final_qty,
            'price': 0,
            'cost': 0
        })
    
    return materials_list

def format_number(num):
    """Форматирование числа с разделителями"""
    return f"{num:,.2f}".replace(",", " ")

def format_materials(materials):
    """Форматирование списка материалов"""
    result = ""
    for i, m in enumerate(materials, 1):
        result += f"{i}. {m['name']}: {format_number(m['qty'])} шт"
        if m['price'] > 0:
            result += f" × {format_number(m['price'])}₽ = {format_number(m['qty'] * m['price'])}₽\n"
        else:
            result += " (цена не указана)\n"
    return result

def format_money_block(data):
    """Форматирование блока Деньги"""
    result = f"💰 *ДЕНЬГИ*\n\n"
    result += f"*ИТОГО за {data['qty']} шт:*\n"
    result += f"Материалы: {format_number(data['materialCost'])}₽\n"
    result += f"Производство: {format_number(data['prodCost'])}₽\n"
    result += f"Чертежи: {format_number(data['drawingCost'])}₽\n"
    result += f"Себестоимость: {format_number(data['totalCost'])}₽\n"
    result += f"Выручка: {format_number(data['revenue'])}₽\n"
    result += f"Прибыль до налога: {format_number(data['profitBeforeTax'])}₽\n"
    result += f"Налог ({data['taxRate']}%): {format_number(data['tax'])}₽\n"
    result += f"*Прибыль после налога: {format_number(data['profitAfterTax'])}₽*\n"
    
    if data['qty'] > 0:
        per_unit_material = data['materialCost'] / data['qty']
        per_unit_total = data['totalCost'] / data['qty']
        per_unit_profit = data['profitAfterTax'] / data['qty']
        
        result += f"\n*НА 1 ШТУКУ:*\n"
        result += f"Материалы: {format_number(per_unit_material)}₽\n"
        result += f"Себестоимость: {format_number(per_unit_total)}₽\n"
        result += f"Прибыль: {format_number(per_unit_profit)}₽\n"
    
    return result

# ==================== ОБРАБОТЧИКИ КОМАНД ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка команды /start"""
    if not await check_access(update):
        return
    
    user_id = update.effective_user.id
    if user_id in sessions:
        del sessions[user_id]
    
    data = load_from_yandex()
    
    if not data['nomenclature']:
        await update.message.reply_text(
            "❌ Не удалось загрузить данные из Яндекс Таблицы.\n"
            "Проверьте ссылку в настройках."
        )
        return
    
    # Получаем уникальные категории
    categories = list(set(
        item.get('Категория', 'Без категории') 
        for item in data['nomenclature'] 
        if item.get('Категория')
    ))
    
    if not categories:
        categories = ["Все изделия"]
    
    # Создаем клавиатуру
    keyboard = []
    for cat in categories[:10]:  # Ограничим 10 категориями
        keyboard.append([InlineKeyboardButton(cat, callback_data=f"cat_{cat}")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
    
    await update.message.reply_text(
        "👋 *Производственный калькулятор*\n\n"
        "Выберите категорию для расчета:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
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
        await query.edit_message_text(
            "❌ Расчет отменен. Используйте /start для нового расчета."
        )
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
            f"📊 *Параметры расчета*\n"
            f"Категория: {category}\n\n"
            f"Введите через пробел:\n"
            f"*Эффективность (%) Налог (%)*\n\n"
            f"Пример: `150 20`",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return
    
    if data == "back_to_categories":
        if user_id in sessions:
            del sessions[user_id]
        
        data = load_from_yandex()
        categories = list(set(
            item.get('Категория', 'Без категории') 
            for item in data['nomenclature'] 
            if item.get('Категория')
        ))
        
        keyboard = []
        for cat in categories[:10]:
            keyboard.append([InlineKeyboardButton(cat, callback_data=f"cat_{cat}")])
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel")])
        
        await query.edit_message_text(
            "👋 *Производственный калькулятор*\n\n"
            "Выберите категорию:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений"""
    if not await check_access(update):
        return
    
    user_id = update.effective_user.id
    session = sessions.get(user_id)
    
    if not session:
        await update.message.reply_text(
            "Используйте /start для начала расчета"
        )
        return
    
    text = update.message.text
    
    if session['step'] == 'parameters':
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text(
                "❌ Введите через пробел: *Эффективность Налог*\n"
                "Пример: `150 20`",
                parse_mode='Markdown'
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
        
        data = load_from_yandex()
        products = [
            item['Наименование'] for item in data['nomenclature']
            if item.get('Тип') in ['Изделие', 'Узел']
        ]
        products.sort()
        
        products_list = "\n".join([f"• {p}" for p in products[:20]])
        if len(products) > 20:
            products_list += f"\n... и еще {len(products) - 20}"
        
        await update.message.reply_text(
            f"✅ Параметры сохранены:\n"
            f"Эффективность: {efficiency:.0f}%\n"
            f"Налог: {tax:.0f}%\n\n"
            f"📋 *Доступные изделия:*\n{products_list}\n\n"
            f"Введите название изделия или узла:",
            parse_mode='Markdown'
        )
        return
    
    elif session['step'] == 'product_selection':
        data = load_from_yandex()
        product = find_product(text, data['nomenclature'])
        
        if not product:
            await update.message.reply_text(
                f"❌ Изделие '{text}' не найдено. Попробуйте еще раз."
            )
            return
        
        session.update({
            'step': 'quantities',
            'product': product,
            'qty': None
        })
        
        output_per_drawing = product.get('Выход_с_чертежа', 1)
        
        await update.message.reply_text(
            f"✅ Выбрано: *{product['Наименование']}*\n"
            f"Кратность: {output_per_drawing}\n\n"
            f"💰 Введите через пробел:\n"
            f"*Рыночная цена Стоимость чертежа изделия*\n\n"
            f"Пример: `3200000 6900000`",
            parse_mode='Markdown'
        )
        return
    
    elif session['step'] == 'quantities':
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text(
                "❌ Введите через пробел: *Рыночная цена Стоимость чертежа*\n"
                "Пример: `3200000 6900000`"
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
        
        product = session['product']
        output_per_drawing = product.get('Выход_с_чертежа', 1)
        
        # Проверка кратности
        if qty % output_per_drawing != 0:
            await update.message.reply_text(
                f"⚠️ Количество должно быть кратно {output_per_drawing}"
            )
            return
        
        drawings_needed = qty // output_per_drawing
        
        data = load_from_yandex()
        materials_dict = collect_materials(
            product['Код'], 1, 
            data['nomenclature'], 
            data['specifications']
        )
        
        materials_list = calculate_materials(
            materials_dict, qty, drawings_needed, session['efficiency']
        )
        
        # ВРЕМЕННО: используем тестовые цены
        material_cost = 0
        for m in materials_list:
            m['price'] = 100
            m['cost'] = m['qty'] * 100
            material_cost += m['cost']
        
        prod_cost = product.get('Фикс_производство', 0) * drawings_needed
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
            f"📊 *РЕЗУЛЬТАТЫ РАСЧЕТА*\n\n"
            f"Изделие: {product['Наименование']}\n"
            f"Количество: {qty:.0f} шт\n"
            f"Эффективность: {session['efficiency']:.0f}%\n"
            f"Налог: {session['tax']:.0f}%\n\n"
            f"*Материалы:*\n{materials_text}\n"
            f"{money_text}",
            parse_mode='Markdown'
        )
        
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
        logger.info(f"URL таблицы: {YANDEX_TABLE_URL}")
        
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

if __name__ == "__main__":
    main()
