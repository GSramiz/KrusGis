import ee
import gspread
import json
import os
import traceback
from oauth2client.service_account import ServiceAccountCredentials

def log_error(context, error):
    """Логирование ошибок с деталями"""
    print(f"\n❌ ОШИБКА в {context}:")
    print(f"Тип: {type(error).__name__}")
    print(f"Сообщение: {str(error)}")
    print("Стек вызовов:")
    traceback.print_exc()
    print("="*50)

def initialize_services():
    """Инициализация сервисов с улучшенным логированием"""
    try:
        print("\n🔧 Инициализация сервисов...")
        
        # 1. Проверка переменной окружения
        if "GEE_CREDENTIALS" not in os.environ:
            raise ValueError("Отсутствует переменная GEE_CREDENTIALS")
        
        # 2. Загрузка credentials
        try:
            service_account_info = json.loads(os.environ["GEE_CREDENTIALS"])
            required_fields = ['client_email', 'private_key', 'token_uri']
            for field in required_fields:
                if field not in service_account_info:
                    raise ValueError(f"Отсутствует обязательное поле: {field}")
        except json.JSONDecodeError as e:
            raise ValueError("Невалидный JSON в GEE_CREDENTIALS") from e

        # 3. Инициализация Earth Engine
        try:
            credentials = ee.ServiceAccountCredentials(
                service_account_info['client_email'],
                key_data=json.dumps(service_account_info)
            )
            ee.Initialize(credentials)
            print("✅ Earth Engine: успешная инициализация")
        except ee.EEException as e:
            raise Exception(f"Ошибка инициализации Earth Engine: {str(e)}") from e

        # 4. Инициализация Google Sheets
        try:
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            sheets_client = gspread.authorize(
                ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
            )
            print("✅ Google Sheets: успешная авторизация")
            return sheets_client
        except Exception as e:
            raise Exception(f"Ошибка авторизации Google Sheets: {str(e)}") from e

    except Exception as e:
        log_error("initialize_services", e)
        raise

def get_best_image(start_date, end_date, geometry, max_clouds=30):
    """Поиск снимка с минимальной облачностью"""
    try:
        print(f"\n🔍 Поиск снимка за период {start_date} - {end_date}")
        
        collection = ee.ImageCollection('COPERNICUS/S2_SR') \
            .filterDate(start_date, end_date) \
            .filterBounds(geometry) \
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', max_clouds)) \
            .sort('CLOUDY_PIXEL_PERCENTAGE')

        if collection.size().getInfo() == 0:
            print("⚠️ Нет подходящих снимков в указанный период")
            return None, None

        best_image = ee.Image(collection.first())
        cloud_percent = best_image.get('CLOUDY_PIXEL_PERCENTAGE').getInfo()
        
        print(f"🌤 Найден снимок с облачностью {cloud_percent}%")
        return best_image, cloud_percent

    except Exception as e:
        log_error("get_best_image", e)
        return None, None

def generate_thumbnail_url(image, geometry, bands=['B4', 'B3', 'B2'], min=0, max=3000):
    """Генерация URL превью с обработкой ошибок"""
    try:
        print("\n🖼 Генерация URL превью...")
        url = image.getThumbURL({
            'bands': bands,
            'min': min,
            'max': max,
            'region': geometry
        })
        print(f"✅ URL превью сгенерирован (длина: {len(url)} символов)")
        return url
    except Exception as e:
        log_error("generate_thumbnail_url", e)
        return None

def update_sheet(sheets_client):
    """Обновление Google-таблицы с детальным логированием"""
    try:
        print("\n📊 Начало обновления таблицы")
        
        config = {
            "spreadsheet_id": "1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY",
            "sheet_name": "1945273513",
            "geometry": ee.Geometry.Rectangle([30, 50, 180, 80])
        }

        # 1. Открытие таблицы
        try:
            print(f"Открываю таблицу {config['spreadsheet_id']}...")
            spreadsheet = sheets_client.open_by_key(config["spreadsheet_id"])
            print(f"✅ Таблица найдена: '{spreadsheet.title}'")
        except gspread.SpreadsheetNotFound:
            raise Exception(f"Таблица с ID {config['spreadsheet_id']} не найдена")
        except Exception as e:
            raise Exception(f"Ошибка доступа к таблице: {str(e)}")

        # 2. Открытие листа
        try:
            worksheet = spreadsheet.worksheet(config["sheet_name"])
            print(f"✅ Лист найден: '{worksheet.title}'")
        except gspread.WorksheetNotFound:
            raise Exception(f"Лист '{config['sheet_name']}' не найден в таблице")

        # 3. Тестовое обновление
        try:
            print("\n🧪 Выполняю тестовое обновление...")
            worksheet.update_cell(1, 1, "Тест из GitHub Actions")
            print("✅ Тестовое обновление выполнено")
        except Exception as e:
            raise Exception(f"Ошибка записи в таблицу: {str(e)}")

        # 4. Основная обработка данных
        try:
            print("\n📝 Обработка данных...")
            data = worksheet.get_all_values()
            
            if not data:
                print("⚠️ Таблица пуста")
                return

            for row_idx, row in enumerate(data[1:], start=2):
                if not row[0]:
                    continue

                try:
                    month, year = row[0].split()
                    start_date = f"{year}-{month}-01"
                    end_date = ee.Date(start_date).advance(1, 'month').format('YYYY-MM-dd').getInfo()

                    print(f"\n🔍 Обрабатываю {month} {year}...")
                    best_image, cloud_percent = get_best_image(start_date, end_date, config["geometry"])
                    
                    if best_image:
                        url = generate_thumbnail_url(best_image, config["geometry"])
                        if url:
                            worksheet.update_cell(row_idx, 2, url)
                            print(f"✅ Обновлено: {url[:50]}...")
                        else:
                            worksheet.update_cell(row_idx, 2, "Ошибка генерации URL")
                    else:
                        worksheet.update_cell(row_idx, 2, "Нет подходящих снимков")

                except Exception as e:
                    log_error(f"обработка строки {row_idx}", e)
                    worksheet.update_cell(row_idx, 2, f"Ошибка: {str(e)[:100]}")

        except Exception as e:
            raise Exception(f"Ошибка обработки данных: {str(e)}") from e

        print("\n🎉 Таблица успешно обновлена")

    except Exception as e:
        log_error("update_sheet", e)
        raise

if __name__ == "__main__":
    try:
        print("\n🚀 Запуск скрипта")
        client = initialize_services()
        if client:
            update_sheet(client)
        print("\n✅ Скрипт успешно завершен")
    except Exception as e:
        log_error("основной поток", e)
        print("\n💥 Критическая ошибка - выполнение прервано")
        exit(1)
