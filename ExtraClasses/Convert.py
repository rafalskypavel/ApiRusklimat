import logging
import os
from datetime import datetime
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
        try:
            # Загрузка данных из JSON файла
            with open(self.json_file_path, 'r', encoding='utf-8-sig') as file:
                data = ujson.load(file)

            # Извлечение данных
            for item in data:
                if 'data' in item:
                    self.data_list.extend(item['data'])
            logging.info(f"Данные успешно извлечены. Всего товаров: {len(self.data_list)}")

        except Exception as e:
            logging.error(f"Ошибка при извлечении данных из JSON файла: {str(e)}")

    def process_data(self):
        def join_with_semicolon(items):
            try:
                return ';'.join(map(str, items))
            except Exception as e:
                logging.error(f"Error joining items with semicolon: {e}")
                return ''

        def get_text_from_dict(data, delimiter=':'):
            try:
                return ';'.join(f"{key}{delimiter}{value}" for key, value in data.items())
            except Exception as e:
                logging.error(f"Error converting dictionary to text: {e}")
                return ''

        if len(self.data_list) == 0:
            logging.warning("Товаров для конвертации нет. Процесс завершен.")
            return []

        converted_data = []

        for item in self.data_list:
            try:
                properties_data = item.get('properties', {})
                properties_text = get_text_from_dict(properties_data)

                remains_data = item.get('remains', {})
                warehouses_text = get_text_from_dict(remains_data.get('warehouses', {}))
                total_text = remains_data.get('total', '')

                # Преобразование словарей для relatedProducts и analog
                related_products_data = item.get('relatedProducts', {})
                related_products_text = get_text_from_dict(related_products_data)

                analog_data = item.get('analog', {})
                analog_text = get_text_from_dict(analog_data)

                item_data = {
                    'id': item.get('id', ''),
                    'nsCode': item.get('nsCode', ''),
                    'categoryId': item.get('categoryId', ''),
                    'vendorCode': item.get('vendorCode', ''),
                    'brand': item.get('brand', ''),
                    'name': item.get('name', ''),
                    'properties': properties_text,
                    'pictures': join_with_semicolon(item.get('pictures', [])),
                    'video': join_with_semicolon(item.get('video', [])),
                    'relatedProducts': related_products_text,
                    'analog': analog_text,
                    'description': item.get('description', ''),
                    'drawing': join_with_semicolon(item.get('drawing', [])),
                    'certificates': join_with_semicolon(item.get('certificates', [])),
                    'promoMaterials': join_with_semicolon(item.get('promoMaterials', [])),
                    'instructions': join_with_semicolon(item.get('instructions', [])),
                    'barcode': join_with_semicolon(map(str, item.get('barcode', []))),
                    'price': item.get('price', 0),
                    'internetPrice': item.get('internetPrice', 0),
                    'clientPrice': item.get('clientPrice', 0),
                    'exclusive': item.get('exclusive', False),
                    'warehouses': warehouses_text,
                    'total': total_text
                }

                converted_data.append(item_data)

            except KeyError as e:
                logging.error(f"KeyError encountered: {e} in item {item}")
            except TypeError as e:
                logging.error(f"TypeError encountered: {e} in item {item}")
            except Exception as e:
                logging.error(f"Unexpected error: {e} in item {item}")

        return converted_data

    def save_to_csv(self, converted_data):
        # Проверка, есть ли данные для сохранения
        if len(converted_data) == 0:
            logging.warning("Нет данных для сохранения в CSV файл.")
            return

        # Проверяем, существует ли файл
        file_exists = os.path.exists(self.csv_file_path)
        previous_size = os.path.getsize(self.csv_file_path) if file_exists else 0
        previous_mod_time = os.path.getmtime(self.csv_file_path) if file_exists else None

        # Логирование информации о текущем состоянии файла до изменения
        if file_exists:
            previous_mod_time_str = datetime.fromtimestamp(previous_mod_time).strftime('%Y-%m-%d %H:%M:%S') if previous_mod_time else 'неизвестно'
            logging.info(f"Текущий размер файла {self.csv_file_path}: {previous_size} байт.")
            logging.info(f"Текущая дата последнего изменения файла {self.csv_file_path}: {previous_mod_time_str}.")
        else:
            logging.info(f"Файл {self.csv_file_path} не существует, он будет создан.")

        try:
            # Преобразуем данные в DataFrame и сохраняем в CSV файл
            result_df = pd.DataFrame(converted_data)
            result_df.to_csv(self.csv_file_path, index=False, encoding='utf-8-sig', sep=";")

            logging.info(f'Данные были успешно конвертированы и сохранены в {self.csv_file_path}')
            logging.info(f'Total items: {len(converted_data)}')

            # Проверка изменений файла: по размеру и дате последнего обновления
            new_size = os.path.getsize(self.csv_file_path)
            new_mod_time = os.path.getmtime(self.csv_file_path)
            new_mod_time_str = datetime.fromtimestamp(new_mod_time).strftime('%Y-%m-%d %H:%M:%S')

            # Логирование изменений размера файла
            if new_size != previous_size:
                logging.info(f"Файл {self.csv_file_path} был обновлен. Старый размер: {previous_size} байт, Новый размер: {new_size} байт.")
            else:
                logging.info(f"Размер файла {self.csv_file_path} не изменился.")

            # Логирование изменений даты последнего обновления
            if new_mod_time != previous_mod_time:
                logging.info(f"Файл {self.csv_file_path} был перезаписан. Новая дата последнего изменения: {new_mod_time_str}.")
            else:
                logging.warning(f"Файл {self.csv_file_path} не был перезаписан.")

        except Exception as e:
            logging.error(f'Конвертация не удалась! Ошибка: {str(e)}')



