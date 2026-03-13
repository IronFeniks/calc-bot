import pandas as pd
import os
import logging
from typing import Dict, List, Tuple, Optional
from functools import lru_cache
import time

logger = logging.getLogger(__name__)

class ExcelReader:
    """Класс для чтения данных из Excel файла (для бота-калькулятора)"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df_nomenclature = None
        self.df_specifications = None
        self.last_load_time = 0
        self.cache_ttl = 300  # 5 минут кэширование
        self.load_data()
    
    def load_data(self) -> Tuple[bool, str]:
        """Загружает данные из Excel файла"""
        try:
            if not os.path.exists(self.file_path):
                return False, f"❌ Файл не найден: {self.file_path}"
            
            # Проверяем, нужно ли перезагружать
            current_time = time.time()
            if self.df_nomenclature is not None and (current_time - self.last_load_time) < self.cache_ttl:
                return True, "✅ Данные из кэша"
            
            # Читаем все листы
            excel_file = pd.ExcelFile(self.file_path)
            
            if 'Номенклатура' not in excel_file.sheet_names:
                return False, "❌ В файле нет листа 'Номенклатура'"
            
            if 'Спецификации' not in excel_file.sheet_names:
                return False, "❌ В файле нет листа 'Спецификации'"
            
            self.df_nomenclature = pd.read_excel(excel_file, sheet_name='Номенклатура')
            self.df_specifications = pd.read_excel(excel_file, sheet_name='Спецификации')
            
            # Заполняем NaN пустыми строками
            self.df_nomenclature = self.df_nomenclature.fillna('')
            self.df_specifications = self.df_specifications.fillna('')
            
            self.last_load_time = current_time
            
            logger.info(f"✅ Загружено: {len(self.df_nomenclature)} записей номенклатуры, {len(self.df_specifications)} спецификаций")
            return True, "✅ Данные загружены"
            
        except Exception as e:
            logger.error(f"Ошибка загрузки Excel: {e}")
            return False, f"❌ Ошибка загрузки: {e}"
    
    def force_reload(self) -> Tuple[bool, str]:
        """Принудительная перезагрузка данных"""
        self.last_load_time = 0  # Сбрасываем время
        return self.load_data()
    
    def get_all_products(self) -> List[Dict]:
        """Возвращает все изделия и узлы"""
        mask = self.df_nomenclature['Тип'].str.lower().isin(['изделие', 'узел'])
        return self.df_nomenclature[mask].to_dict('records')
    
    def get_product_by_code(self, code: str) -> Optional[Dict]:
        """Возвращает продукт по коду"""
        mask = self.df_nomenclature['Код'] == code
        if mask.any():
            return self.df_nomenclature[mask].iloc[0].to_dict()
        return None
    
    def get_materials(self) -> List[Dict]:
        """Возвращает все материалы"""
        mask = self.df_nomenclature['Тип'].str.lower() == 'материал'
        return self.df_nomenclature[mask].to_dict('records')
    
    def get_specifications(self, parent_code: str) -> List[Dict]:
        """Возвращает спецификации для родителя"""
        mask = self.df_specifications['Родитель'] == parent_code
        return self.df_specifications[mask].to_dict('records')
    
    def build_category_tree(self) -> Dict:
        """Строит дерево категорий"""
        tree = {}
        
        for _, item in self.df_nomenclature.iterrows():
            category_str = item.get('Категории', '')
            if not category_str or pd.isna(category_str):
                continue
            
            # Разбиваем категорию на уровни
            path = [cat.strip() for cat in str(category_str).split(' > ')]
            
            current = tree
            for i, cat in enumerate(path):
                if cat not in current:
                    current[cat] = {'_subcategories': {}, '_items': []}
                
                # Если это последний уровень и это изделие/узел
                if i == len(path) - 1 and item['Тип'].lower() in ['изделие', 'узел']:
                    current[cat]['_items'].append({
                        'code': item['Код'],
                        'name': item['Наименование']
                    })
                
                current = current[cat]['_subcategories']
        
        return tree
    
    def get_categories_at_level(self, tree: Dict, path: List[str] = None) -> List[str]:
        """Возвращает подкатегории на уровне"""
        if path is None:
            path = []
        
        current = tree
        for cat in path:
            if cat in current:
                current = current[cat]['_subcategories']
            else:
                return []
        
        return list(current.keys())
    
    def get_items_at_level(self, tree: Dict, path: List[str]) -> List[Dict]:
        """Возвращает изделия на уровне"""
        if not path:
            return []
        
        current = tree
        for cat in path[:-1]:
            if cat in current:
                current = current[cat]['_subcategories']
            else:
                return []
        
        last_cat = path[-1]
        if last_cat in current:
            return current[last_cat].get('_items', [])
        
        return []
    
    def collect_materials(self, product_code: str, multiplier: float = 1.0) -> Dict[str, Dict]:
        """Собирает все материалы для изделия с учетом вложенности"""
        materials = {}
        
        def explode(code: str, mult: float):
            specs = self.get_specifications(code)
            
            for spec in specs:
                child_code = spec['Потомок']
                quantity = float(spec['Количество'])
                
                child = self.get_product_by_code(child_code)
                if not child:
                    continue
                
                if child['Тип'].lower() == 'материал':
                    if child_code not in materials:
                        materials[child_code] = {
                            'name': child['Наименование'],
                            'baseQty': 0
                        }
                    materials[child_code]['baseQty'] += quantity * mult
                
                elif child['Тип'].lower() == 'узел':
                    explode(child_code, mult * quantity)
        
        explode(product_code, multiplier)
        return materials
    
    def get_production_price(self, product_code: str) -> float:
        """Возвращает цену производства"""
        product = self.get_product_by_code(product_code)
        if not product:
            return 0
        
        price_str = str(product.get('Цена производства', '0')).replace(' ISK', '').replace(' ', '')
        try:
            return float(price_str) if price_str else 0
        except:
            return 0
    
    def get_multiplicity(self, product_code: str) -> int:
        """Возвращает кратность производства"""
        product = self.get_product_by_code(product_code)
        if not product:
            return 1
        
        try:
            return int(product.get('Кратность', 1))
        except:
            return 1
