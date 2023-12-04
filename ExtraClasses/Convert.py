import logging

import pandas as pd
import ujson
from configparser import ConfigParser

class Convert:
    def __init__(self, json_file_path, csv_file_path):
        self.config = ConfigParser()
        self.config.read("Properties\\properties.properties")
        self.json_file_path = json_file_path
        self.csv_file_path = csv_file_path
        self.data_list = []
        self.logging = logging.basicConfig(filename=self.config.get("Logging", "filename"), filemode='a', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info("Началась конвертация из JSON в CSV")

    def extract_data(self):
        # Загрузка данных из JSON файла
        with open(self.json_file_path, 'r', encoding='utf-8-sig') as file:
            data = ujson.load(file)

        # Извлечение данных
        for item in data:
            if 'data' in item:
                self.data_list.extend(item['data'])

    def process_data(self):
        converted_data = []

        # в dataList лежат "data"
        for item in self.data_list:
            # Извлечение данных о товаре
            properties_data = item.get('properties', {})

            properties_text = ';'.join([f"{key}:{value}" for key, value in properties_data.items()])

            remains_data = item.get('remains', {})
            warehouses_text = ';'.join([f"{warehouse}:{quantity}" for warehouse, quantity in remains_data.get('warehouses', {}).items()])
            total_text = remains_data.get('total', '')

            item_data = {
                'id': item.get('id', ''),
                'nsCode': item.get('nsCode', ''),
                'categoryId': item.get('categoryId', ''),
                'vendorCode': item.get('vendorCode', ''),
                'brand': item.get('brand', ''),
                'categoryName': item.get('categoryName', ''),
                'name': item.get('name', ''),
                'properties': properties_text,
                'pictures': ';'.join(item.get('pictures', [])),
                'video': ';'.join(item.get('video', [])),
                'relatedProducts': ';'.join(item.get('relatedProducts', [])),
                'analog': ';'.join(item.get('analog', [])),
                'description': item.get('description', ''),
                'drawing': ';'.join(item.get('drawing', [])),
                'certificates': ';'.join(item.get('certificates', [])),
                'promoMaterials': ';'.join(item.get('promoMaterials', [])),
                'instructions': ';'.join(item.get('instructions', [])),
                'barcode': ';'.join(map(str, item.get('barcode', []))),
                'price': item.get('price', 0),
                'internetPrice': item.get('internetPrice', 0),
                'clientPrice': item.get('clientPrice', 0),
                'exclusive': item.get('exclusive', False),
                'warehouses': warehouses_text,
                'total': total_text
            }

            # Добавление данных товара в список
            converted_data.append(item_data)

        return converted_data

    def save_to_csv(self, converted_data):
        try:
            result_df = pd.DataFrame(converted_data)
            result_df.to_csv(self.csv_file_path, index=False, encoding='utf-8-sig', sep=";")
            logging.info(f'Данные были успешно конвертированы и сохранены в {self.csv_file_path}')
            logging.info(f'Total items: {len(converted_data)}')
        except Exception as e:
            logging.error(f'Конвертация не удалась!. Error: {str(e)}')







