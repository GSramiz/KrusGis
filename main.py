import ee
import gspread
import json
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

def initialize_services():
    """Инициализация всех сервисов одним ключом"""
    try:
        # Загружаем ключ из переменных окружения
        service_account_info = json.loads(os.environ["GEE_CREDENTIALS"])
        
        # 1. Инициализация Earth Engine
        credentials = ee.ServiceAccountCredentials(
            service_account_info['client_email'],
            key_data=json.dumps(service_account_info)
        ee.Initialize(credentials)
        print("✅ Earth Engine инициализирован")

        # 2. Инициализация Google Sheets
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        sheets_client = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
        print("✅ Google Sheets авторизован")
        
        return sheets_client

    except Exception as e:
        print(f"❌ Ошибка инициализации: {str(e)}")
        return None

def get_best_image(start_date, end_date, geometry, max_clouds=30):
    """Поиск снимка с минимальной облачностью"""
    try:
        collection = ee.ImageCollection('COPERNICUS/S2_SR') \
            .filterDate(start_date, end_date) \
            .filterBounds(geometry) \
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', max_clouds)) \
            .sort('CLOUDY_PIXEL_PERCENTAGE')

        best_image = ee.Image(collection.first())
        cloud_percent = best_image.get('CLOUDY_PIXEL_PERCENTAGE').getInfo()
        
        if cloud_percent is None:
            return None, None

        print(f"🌤 Найден снимок с облачностью {cloud_percent}%")
        return best_image, cloud_percent

    except Exception as e:
        print(f"❌ Ошибка поиска снимка: {str(e)}")
        return None, None

def generate_thumbnail_url(image, geometry, bands=['B4', 'B3', 'B2'], min=0, max=3000):
    """Генерация URL превью"""
    try:
        return image.getThumbURL({
            'bands': bands,
            'min': min,
            'max': max,
            'region': geometry
        })
    except Exception as e:
        print(f"❌ Ошибка генерации URL: {str(e)}")
        return None

def update_sheet(sheets_client):
    """Обновление Google-таблицы"""
    try:
        # Конфигурация
        config = {
            "spreadsheet_id": "1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY",
            "sheet_name": "1945273513",
            "geometry": ee.Geometry.Rectangle([30, 50, 180, 80])  # Границы РФ
        }

        sheet = sheets_client.open_by_key(config["spreadsheet_id"]).worksheet(config["sheet_name"])
        data = sheet.get_all_values()

        for row_idx, row in enumerate(data[1:], start=2):  # Пропускаем заголовок
            if not row[0]:  # Пустая строка
                continue

            try:
                month, year = row[0].split()
                start_date = f"{year}-{month}-01"
                end_date = ee.Date(start_date).advance(1, 'month').format('YYYY-MM-dd').getInfo()

                print(f"\n🔍 Обрабатываю {month} {year}...")

                # Поиск и обработка снимка
                best_image, cloud_percent = get_best_image(start_date, end_date, config["geometry"])
                
                if best_image:
                    url = generate_thumbnail_url(best_image, config["geometry"])
                    if url:
                        sheet.update_cell(row_idx, 2, url)
                        print(f"✅ Обновлено: {url[:50]}...")
                    else:
                        sheet.update_cell(row_idx, 2, "Ошибка генерации URL")
                else:
                    sheet.update_cell(row_idx, 2, "Нет подходящих снимков")

            except Exception as e:
                print(f"⚠️ Ошибка в строке {row_idx}: {str(e)}")
                sheet.update_cell(row_idx, 2, f"Ошибка: {str(e)}")

    except Exception as e:
        print(f"❌ Критическая ошибка таблицы: {str(e)}")

if __name__ == "__main__":
    client = initialize_services()
    if client:
        update_sheet(client)
    else:
        print("🛑 Не удалось инициализировать сервисы")
