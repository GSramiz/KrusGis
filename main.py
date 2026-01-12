import ee
import gspread
import json
import os
import traceback
import calendar
import time
import random
from oauth2client.service_account import ServiceAccountCredentials

# Конфигурация
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY")
SHEET_NAME = "Sentinel-2 Покрытие"

def log_error(context, error):
    print(f"\ОШИБКА в {context}:")
    print(f"Тип: {type(error).__name__}")
    print(f"Сообщение: {str(error)}")
    traceback.print_exc()
    print("=" * 50)

def retry(func, *args, retries=5, **kwargs):
    """Обёртка для повторных попыток при APIError 500"""
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if i < retries - 1:
                wait = (2 ** i) + random.uniform(0, 1)
                print(f"⚠️ Ошибка {e}, повтор через {wait:.1f} сек...")
                time.sleep(wait)
                continue
            raise

def initialize_services():
    try:
        print("\nИнициализация сервисов...")

        service_account_info = json.loads(os.environ["GEE_CREDENTIALS"])

        credentials = ee.ServiceAccountCredentials(
            service_account_info["client_email"],
            key_data=json.dumps(service_account_info)
        )
        ee.Initialize(credentials)
        print("Earth Engine: инициализирован")

        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        sheets_client = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
        )
        print("Google Sheets: авторизация прошла успешно")
        return sheets_client

    except Exception as e:
        log_error("initialize_services", e)
        raise

def month_str_to_number(name):
    months = {
        "Январь": "01", "Февраль": "02", "Март": "03", "Апрель": "04",
        "Май": "05", "Июнь": "06", "Июль": "07", "Август": "08",
        "Сентябрь": "09", "Октябрь": "10", "Ноябрь": "11", "Декабрь": "12"
    }
    return months.get(name.strip().capitalize(), None)

def get_geometry_from_asset(region_name):
    fc = ee.FeatureCollection("projects/ee-romantik1994/assets/region")
    region = fc.filter(ee.Filter.eq("title", region_name)).first()
    if region is None:
        raise ValueError(f"Регион '{region_name}' не найден в ассете")
    return region.geometry()

def mask_clouds(img):
    scl = img.select("SCL")
    # Оставляем только "чистые" пиксели: 4 (vegetation), 5 (non-vegetated), 6 (water), 7 (unclassified)
    allowed = scl.eq(4).Or(scl.eq(5)).Or(scl.eq(6)).Or(scl.eq(7))
    return img.updateMask(allowed).resample("bilinear")

def update_sheet(sheets_client, batch_size=50):
    try:
        print("Обновление таблицы")

        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        data = worksheet.get_all_values()

        values_to_update = []

        for row_idx, row in enumerate(data[1:], start=2):
            try:
                region, date_str = row[:2]
                if not region or not date_str:
                    values_to_update.append([""])
                    continue

                parts = date_str.strip().split()
                if len(parts) != 2:
                    raise ValueError(f"Неверный формат даты: '{date_str}'")

                month_num = month_str_to_number(parts[0])
                year = parts[1]
                start = f"{year}-{month_num}-01"
                days = calendar.monthrange(int(year), int(month_num))[1]
                end_str = f"{year}-{month_num}-{days:02d}"

                print(f"\n {region} — {start} - {end_str}")

                geometry = get_geometry_from_asset(region)

                collection = (
                    ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                    .filterDate(start, end_str)
                    .filterBounds(geometry)
                    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30))
                    .map(mask_clouds)
                )

                if collection.first().getInfo() is None:
                    values_to_update.append(["Нет снимков"])
                    continue

                filtered_mosaic = collection.median()

                tile_info = retry(ee.data.getMapId, {
                    "image": filtered_mosaic,
                    "bands": ["B4", "B3", "B2"],
                    "min": "0,0,0",
                    "max": "3000,3000,3000"
                })

                clean_mapid = tile_info["mapid"].split("/")[-1]
                xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{clean_mapid}/tiles/{{z}}/{{x}}/{{y}}"

                values_to_update.append([xyz])

            except Exception as e:
                log_error(f"Строка {row_idx}", e)
                values_to_update.append([f"Ошибка: {str(e)[:100]}"])

            # Пакетное обновление каждые batch_size строк
            if len(values_to_update) >= batch_size:
                start_row = row_idx - len(values_to_update) + 1
                retry(worksheet.update, f'C{start_row}:C{row_idx}', values_to_update)
                values_to_update = []
                time.sleep(1)  # пауза между батчами

        # Обновление оставшихся строк
        if values_to_update:
            start_row = len(data) - len(values_to_update) + 1
            end_row = len(data)
            retry(worksheet.update, f'C{start_row}:C{end_row}', values_to_update)
            time.sleep(1)  # пауза

    except Exception as e:
        log_error("update_sheet", e)
        raise

if __name__ == "__main__":
    try:
        client = initialize_services()
        update_sheet(client)
        print("Скрипт успешно завершен")
    except Exception as e:
        log_error("main", e)
        exit(1)
