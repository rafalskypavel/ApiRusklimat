import json
import logging
import os
from configparser import ConfigParser


class ProductMerger:
    def __init__(self, products_file, properties_file, categories_file, output_file):
        """
        Инициализация класса ProductMerger.

        :param products_file: Путь к файлу JSON с продуктами.
        :param properties_file: Путь к файлу JSON со свойствами.
        :param categories_file: Путь к файлу JSON с категориями.
        :param output_file: Путь к выходному файлу JSON.
        """
        self.config = ConfigParser()
        self.config.read("Properties\\properties.properties")
        self.products_file = products_file
        self.properties_file = properties_file
        self.categories_file = categories_file
        self.output_file = output_file

        # Настройка логирования
        logging.basicConfig(
            filename=self.config.get("Logging", "filename"),
            filemode='a',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logging.info("Началась сводка скачанных JSON файлов в один файл JSON")

    def merge_properties_and_categories_into_products(self):
        """
        Объединение данных из файлов продуктов, свойств и категорий в один файл.
        """

        def load_json_file(file_path):
            """
            Загружает данные из файла JSON.

            :param file_path: Путь к файлу JSON.
            :return: Данные из файла или None в случае ошибки.
            """
            if not os.path.isfile(file_path):
                logging.error(f"Файл {file_path} не найден или не является файлом.")
                return None

            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    return json.load(file)
            except FileNotFoundError:
                logging.error(f"Файл {file_path} не найден.")
            except json.JSONDecodeError:
                logging.error(f"Ошибка декодирования JSON в файле {file_path}.")
            except IOError as e:
                logging.error(f"Ошибка ввода/вывода при работе с файлом {file_path}. Error: {str(e)}")
            except Exception as e:
                logging.error(f"Не удалось загрузить данные из {file_path}. Error: {str(e)}")
            return None

        products_data = load_json_file(self.products_file)
        properties_data = load_json_file(self.properties_file)
        categories_data = load_json_file(self.categories_file)

        if products_data is None or properties_data is None or categories_data is None:
            logging.error("Не удалось загрузить один или несколько файлов. Объединение не выполнено.")
            return

        # Создание словарей для быстрого доступа
        try:
            properties_dict = {item["id"]: item["name"] for item in properties_data.get("data", [])}
            categories_dict = {item["id"]: item["name"] for item in categories_data.get("data", [])}
            products_dict = {item["id"]: item for product_group in products_data for item in
                             product_group.get("data", [])}
        except KeyError as e:
            logging.error(f"Ошибка в данных свойств или категорий. Возможно отсутствует ключ: {str(e)}.")
            return
        except TypeError as e:
            logging.error(f"Ошибка обработки данных JSON. Возможно, данные не в том формате. Error: {str(e)}.")
            return

        # Обновление данных в products.json
        for product_group in products_data:
            for product in product_group.get("data", []):
                try:
                    # Обновление данных в поле "properties"
                    product_properties = product.get("properties", {})
                    updated_properties = {properties_dict.get(prop_id, prop_id): value
                                          for prop_id, value in product_properties.items()}
                    product["properties"] = updated_properties

                    # Обновление данных в поле "categoryId" на имя категории
                    category_id = product.get("categoryId")
                    product["categoryId"] = categories_dict.get(category_id, category_id)

                    # Обновление данных в поле "relatedProducts"
                    related_products = product.get("relatedProducts", [])
                    updated_related_products = {}
                    for related_id in related_products:
                        related_product = products_dict.get(related_id)
                        if related_product:
                            updated_related_products[related_product["nsCode"]] = related_product["name"]
                        else:
                            logging.warning(f"Связанный продукт с ID {related_id} не найден в данных продуктов.")
                    product["relatedProducts"] = updated_related_products

                    # Обновление данных в поле "analog"
                    analog = product.get("analog", [])
                    updated_analog = {}
                    for related_id in analog:
                        related_product = products_dict.get(related_id)
                        if related_product:
                            updated_analog[related_product["nsCode"]] = related_product["name"]
                        else:
                            logging.warning(f"Связанный продукт с ID {related_id} не найден в данных продуктов.")
                    product["analog"] = updated_analog

                except KeyError as e:
                    logging.error(
                        f"Ошибка в данных продукта {product.get('id', 'неизвестен')}. Возможно отсутствует ключ: {str(e)}.")
                except TypeError as e:
                    logging.error(
                        f"Ошибка обработки данных продукта {product.get('id', 'неизвестен')}. Возможно, данные не в том формате. Error: {str(e)}.")
                except Exception as e:
                    logging.error(
                        f"Ошибка при обновлении данных продукта {product.get('id', 'неизвестен')}. Error: {str(e)}")

        # Запись обновленных данных в новый файл
        try:
            with open(self.output_file, 'w', encoding='utf-8') as output_json_file:
                json.dump(products_data, output_json_file, ensure_ascii=False, indent=4)
            logging.info(f"Данные из файлов {os.path.basename(self.products_file)}, "
                         f"{os.path.basename(self.properties_file)}, "
                         f"{os.path.basename(self.categories_file)} успешно объединены и сохранены в {self.output_file}.")
        except IOError as e:
            logging.error(f'Ошибка ввода/вывода при сохранении объединенных данных. Error: {str(e)}')
        except Exception as e:
            logging.error(f'Не удалось сохранить объединенные данные. Error: {str(e)}')
