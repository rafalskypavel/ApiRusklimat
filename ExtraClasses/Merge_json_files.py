import json
import logging
import os
from configparser import ConfigParser

class ProductMerger:
    def __init__(self, products_file, properties_file, categories_file, output_file):
        self.config = ConfigParser()
        self.config.read("Properties\\properties.properties")
        self.products_file = products_file
        self.properties_file = properties_file
        self.categories_file = categories_file
        self.output_file = output_file
        self.logging = logging.basicConfig(filename=self.config.get("Logging", "filename"), filemode='a', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info("Началась сводка скачанных JSON файлов в один файл JSON")

    def merge_properties_and_categories_into_products(self):
        # Чтение данных из файлов
        with open(self.products_file, 'r', encoding='utf-8') as products_json_file:
            products_data = json.load(products_json_file)

        with open(self.properties_file, 'r', encoding='utf-8') as properties_json_file:
            properties_data = json.load(properties_json_file)

        with open(self.categories_file, 'r', encoding='utf-8') as categories_json_file:
            categories_data = json.load(categories_json_file)

        # Создание словарей для быстрого доступа к данным из properties.json и categories.json
        properties_dict = {item["id"]: item["name"] for item in properties_data.get("data", [])}
        categories_dict = {item["id"]: item for item in categories_data.get("data", [])}

        # Обновление данных в products.json
        for product_group in products_data:
            for product in product_group.get("data", []):
                # Обновление данных в поле "properties"
                product_properties = product.get("properties", {})
                updated_properties = {}

                for property_id, value in product_properties.items():
                    if property_id in properties_dict:
                        updated_properties[properties_dict[property_id]] = value
                    else:
                        updated_properties[property_id] = value

                product["properties"] = updated_properties

                # Обновление данных в поле "categoryId"
                category_id = product.get("categoryId")
                if category_id in categories_dict:
                    product["categoryName"] = categories_dict[category_id]["name"]
                else:
                    product["categoryName"] = None

        # Запись обновленных данных в новый файл
        with open(self.output_file, 'w', encoding='utf-8') as output_json_file:
            try:
                json.dump(products_data, output_json_file, ensure_ascii=False, indent=4)
                logging.info(f"Данные {os.path.basename(self.products_file)}, "
                             f"{os.path.basename(self.properties_file)}, "
                             f"{os.path.basename(self.categories_file)} были успешно объединены "
                             f"и сохранены в {self.output_file}")
            except Exception as e:
                logging.error(f'Объединение JSON файлов не удалось!. Error: {str(e)}')