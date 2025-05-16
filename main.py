import ee
import gspread
import json
import os
import traceback
from oauth2client.service_account import ServiceAccountCredentials

# Логирование ошибок
def log_error(context, error):
    print(f"\n❌ ОШИБКА в {context}:")
    print(f"Тип: {type(error).__name__}")
    print(f"Сообщение: {str(error)}")
    traceback.print_exc()
    print("=" * 50)

# Инициализация Earth Engine и Google Sheets
def initialize_services():
    try:
        print("\n🔧 Инициализация сервисов...")

        service_account_info = json.loads(os.environ["GEE_CREDENTIALS"])

        credentials = ee.ServiceAccountCredentials(
            service_account_info["client_email"],
            key_data=json.dumps(service_account_info)
        )
        ee.Initialize(credentials)
        print("✅ Earth Engine: инициализирован")

        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        sheets_client = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
        )
        print("✅ Google Sheets: авторизация прошла успешно")
        return sheets_client

    except Exception as e:
        log_error("initialize_services", e)
        raise

# Перевод месяца из строки в номер
def month_str_to_number(name):
    months = {
        "Январь": "01", "Февраль": "02", "Март": "03", "Апрель": "04",
        "Май": "05", "Июнь": "06", "Июль": "07", "Август": "08",
        "Сентябрь": "09", "Октябрь": "10", "Ноябрь": "11", "Декабрь": "12"
    }
    return months.get(name.strip().capitalize(), None)

# Получение геометрии региона
def get_geometry_from_asset(region_name):
    fc = ee.FeatureCollection("projects/ee-romantik1994/assets/region")
    region = fc.filter(ee.Filter.eq("title", region_name)).first()
    if region is None:
        raise ValueError(f"Регион '{region_name}' не найден в ассете")
    return region.geometry()

# Маскирование облаков по SCL
def mask_clouds(img):
    scl = img.select("SCL")
    cloud_mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    return img.updateMask(cloud_mask)

# Растяжка значений по перцентилям
def stretch_visualization(image, geometry):
    stats = image.reduceRegion(
        reducer=ee.Reducer.percentile([2, 98]),
        geometry=geometry,
        scale=20,
        bestEffort=True
    )

    bands = ["B4", "B3", "B2"]
    min_vals = [stats.get(f"{b}_p2") for b in bands]
    max_vals = [stats.get(f"{b}_p98") for b in bands]

    return image.visualize(
        bands=bands,
        min=min_vals,
        max=max_vals
    )

# Обновление таблицы
def update_sheet(sheets_client):
    try:
        print("\n📊 Обновление таблицы")

        SPREADSHEET_ID = "1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY"
        SHEET_NAME = "Sentinel-2 Покрытие"

        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        data = worksheet.get_all_values()

        for row_idx, row in enumerate(data[1:], start=2):
            try:
                region, date_str = row[:2]
                if not region or not date_str:
                    continue

                parts = date_str.strip().split()
                if len(parts) != 2:
                    raise ValueError(f"Неверный формат даты: '{date_str}'")

                month_num = month_str_to_number(parts[0])
                year = parts[1]
                start = f"{year}-{month_num}-01"
                end = ee.Date(start).advance(1, "month")

                print(f"\n🌍 {region} — {start} - {end.format('YYYY-MM-dd').getInfo()}")

                geometry = get_geometry_from_asset(region)

                # Коллекция Sentinel-2 с маской облаков и выбором RGB-каналов
                collection = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED") \
                    .filterDate(start, end) \
                    .filterBounds(geometry) \
                    .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 40)) \
                    .map(mask_clouds) \
                    .map(lambda img: img.select(["B2", "B3", "B4", "B8"]))

                if collection.size().getInfo() == 0:
                    worksheet.update_cell(row_idx, 3, "Нет снимков")
                    continue

                # Выбор наиболее ярких пикселей на основе NIR (B8)
                best = collection.qualityMosaic("B8")

                # Растяжка визуализации по перцентилям
                visual = stretch_visualization(best, geometry).clip(geometry)

                tile_info = ee.data.getMapId({"image": visual})
                mapid = tile_info["mapid"]
                xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{mapid}/tiles/{{z}}/{{x}}/{{y}}"

                worksheet.update_cell(row_idx, 3, xyz)

            except Exception as e:
                log_error(f"Строка {row_idx}", e)
                worksheet.update_cell(row_idx, 3, f"Ошибка: {str(e)[:100]}")

    except Exception as e:
        log_error("update_sheet", e)
        raise

# Точка входа
if __name__ == "__main__":
    try:
        client = initialize_services()
        update_sheet(client)
        print("\n✅ Скрипт успешно завершен")
    except Exception as e:
        log_error("main", e)
        exit(1)
