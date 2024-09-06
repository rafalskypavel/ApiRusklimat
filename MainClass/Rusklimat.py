import json
import requests
import cloudscraper
from configparser import ConfigParser
import logging
from ExtraClasses.Convert import Convert
from ExtraClasses.Merge_json_files import ProductMerger


class Api_rusklimat:
    def __init__(self):
        # Инициализация параметров аутентификации и путей
        self.config = ConfigParser()
        self.config.read("Properties\\properties.properties")
        self.login = self.config.get("Auth", "login")
        self.password = self.config.get("Auth", "password")
        self.partnerId = self.config.get("Auth", "partnerId")
        self.path_for_json_response_files = self.config.get("Path", "path_for_json_response_files")

        # Настройка логирования
        self.logging = logging.basicConfig(
            filename=self.config.get("Logging", "filename"),
            filemode='w',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

        # Базовые URL для работы с API
        self.base_url = "https://b2b.rusklimat.com"
        self.partner_url = "https://internet-partner.rusklimat.com/api/v1/InternetPartner"

        # Инициализация скраппера
        self.scraper = cloudscraper.create_scraper()

        # Получение токенов и ключей
        self.jwt_token = self._get_jwt_token()
        self.get_request_key = self._get_request_key()

    def _get_jwt_token(self):
        """Метод для получения JWT токена с аутентификацией по логину и паролю."""
        url = f"{self.base_url}/api/v1/auth/jwt/"
        headers = {"User-Agent": "catalog-ip"}
        data = {"login": self.login, "password": self.password}

        try:
            response = self.scraper.post(url, headers=headers, json=data)
            if response.status_code == 200:
                self.jwtToken = response.json().get("data", {}).get("jwtToken")
                logging.info('JWT token успешно получен!')
                return self.jwtToken
            elif response.status_code == 401:
                logging.error(f"Неверный логин или пароль. Status code: {response.status_code}")
                return None
            else:
                logging.error(
                    f"JWT token получить не удалось. Status code: {response.status_code}, Response: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при запросе JWT токена: {str(e)}")
            return None

    def _get_request_key(self):
        """Метод для получения requestKey с использованием OAuth."""
        headers = {"Authorization": f"OAuth {self.jwt_token}"}
        request_url = f"{self.partner_url}/{self.partnerId}/requestKey/"

        try:
            response = requests.get(request_url, headers=headers)
            if response.status_code == 200:
                requestKey = response.json().get("requestKey")
                logging.info('requestKey успешно получен!')
                return requestKey
            else:
                logging.error(
                    f"requestKey получить не удалось. Status code: {response.status_code}, Response: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при запросе requestKey: {str(e)}")
            return None

    def get_categories(self):
        """Метод для получения списка категорий продуктов."""
        headers = {"Authorization": f"OAuth {self.jwt_token}"}

        try:
            response = requests.get(f"{self.partner_url}/categories/{self.get_request_key}", headers=headers)
            if response.status_code == 200:
                logging.info('Начался парсинг категорий!')
                categories = response.json()
                logging.info('Категории успешно получены!')
                return categories
            else:
                logging.error(
                    f"Начать парсинг категорий не удалось. Status code: {response.status_code}, Response: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при запросе категорий: {str(e)}")
            return None

    def get_properties(self):
        """Метод для получения характеристик товаров."""
        headers = {"Authorization": f"OAuth {self.jwt_token}"}

        try:
            response = requests.get(f"{self.partner_url}/properties/{self.get_request_key}", headers=headers)
            if response.status_code == 200:
                logging.info('Начался парсинг характеристики товаров!')
                properties = response.json()
                logging.info('Характеристика товаров успешно получена!')
                return properties
            else:
                logging.error(
                    f"Начать парсинг характеристики товаров не удалось. Status code: {response.status_code}, Response: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при запросе характеристик: {str(e)}")
            return None

    def get_products(self):
        """Метод для получения всех товаров постранично."""
        all_products = []  # Список для хранения данных по всем товарам
        page = 1
        page_size = 500
        logging.info('Начинаем парсить товары!')

        while True:
            url = f"{self.partner_url}/{self.partnerId}/products/{self.get_request_key}/"
            headers = {
                "Authorization": f"OAuth {self.jwt_token}",
                "User-Agent": "catalog-ip"
            }
            params = {
                "pageSize": page_size,
                "page": page
            }

            try:
                response = self.scraper.post(url, headers=headers, params=params, json={})
                if response.status_code == 200:
                    logging.info(f'Страница № {page}')
                    products_data = response.json()
                    all_products.append(products_data)
                    page += 1
                elif response.status_code == 404:
                    logging.info(f'Итого загружено страниц: {page - 1}')
                    break
                else:
                    logging.error(
                        f"Начать парсинг товаров не удалось. Status code: {response.status_code}, Response: {response.text}")
                    break
            except requests.exceptions.RequestException as e:
                logging.error(f"Ошибка при парсинге товаров: {str(e)}")
                break

        return all_products

    def save_to_json(self, data, name: str):
        """Метод для сохранения данных в JSON файл."""
        try:
            with open(f"{self.path_for_json_response_files}{name}", 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)
            logging.info(f'{name.split(".")[0]} успешно сохранены в файл {name}.')
        except Exception as e:
            logging.error(f'Не удалось сохранить {name.split(".")[0]} в файл {name}. Error: {str(e)}')


if __name__ == "__main__":
    try:
        Api_class = Api_rusklimat()

        # Получение категорий и их сохранение
        categories = Api_class.get_categories()
        if categories:
            Api_class.save_to_json(categories, "categories.json")

        # Получение характеристик и их сохранение
        properties = Api_class.get_properties()
        if properties:
            Api_class.save_to_json(properties, "properties.json")

        # Получение товаров и их сохранение
        products = Api_class.get_products()
        if products:
            Api_class.save_to_json(products, "products.json")

        # # Объединение данных и конвертация в CSV
        path = "Output\\Transitional files\\"
        result = "Output\\Result\\"
        products_file = path + "products.json"
        properties_file = path + "properties.json"
        categories_file = path + "categories.json"
        output_file = path + "output_merged.json"

        product_merger = ProductMerger(products_file, properties_file, categories_file, output_file)
        product_merger.merge_properties_and_categories_into_products()

        # Конвертация в CSV
        json_file_path = path + "output_merged.json"
        csv_file_path = result + "output_data.csv"
        converter = Convert(json_file_path, csv_file_path)
        converter.extract_data()
        converted_data = converter.process_data()
        converter.save_to_csv(converted_data)

    except Exception as e:
        logging.error(f'Произошла ошибка в процессе выполнения скрипта: {str(e)}')
