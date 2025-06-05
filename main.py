import ee
import gspread
import json
import os
import traceback
import calendar
from collections import defaultdict
from oauth2client.service_account import ServiceAccountCredentials

# Русские названия месяцев
month_names_ru = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
}

def month_str_to_number(name):
    months = {v: f"{k:02d}" for k, v in month_names_ru.items()}
    return months.get(name.strip().capitalize(), None)

# Конфигурация
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY")
SHEET_NAME = "Sentinel-2 Покрытие"

def log_error(context, error):
    print(f"ОШИБКА в {context}:")
    print(f"Тип: {type(error).__name__}")
    print(f"Сообщение: {str(error)}")
    traceback.print_exc()
    print("=" * 50)

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

def get_geometry_from_asset(region_name):
    fc = ee.FeatureCollection("projects/ee-romantik1994/assets/region")
    region = fc.filter(ee.Filter.eq("title", region_name)).first()
    if region is None:
        raise ValueError(f"Регион '{region_name}' не найден в ассете")
    return region.geometry()

# Новая маска облаков через коллекцию облачности
def mask_clouds(img):
    cloud_prob = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY') \
        .filter(ee.Filter.eq('system:index', img.get('system:index'))).first()
    cloud_mask = cloud_prob.lt(40).rename('cloudmask')
    return img.updateMask(cloud_mask)

def ensure_month_coverage(sheets_client):
    REQUIRED_MONTHS = {'04', '05', '06', '07', '08', '09', '10'}
    YEARS = [str(y) for y in range(2022, 2026)]

    spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet(SHEET_NAME)
    data = worksheet.get_all_values()

    headers = data[0]
    rows = data[1:]
    existing = set()
    grouped = defaultdict(set)
    full_data = []

    for row in rows:
        region = row[0].strip()
        date = row[1].strip() if len(row) > 1 else ""

        if not region or not date or " " not in date:
            continue

        month_name, year = date.split()
        month_num = month_str_to_number(month_name)
        if not month_num or year not in YEARS:
            continue

        key = (region, year, month_num)
        existing.add(key)
        grouped[(region, year)].add(month_num)
        full_data.append((region, year, month_num, row))

    for (region, year), months in grouped.items():
        missing = REQUIRED_MONTHS - months
        for month in missing:
            date_label = f"{month_names_ru[int(month)]} {year}"
            full_data.append((region, year, month, [region, date_label, "", "⛔ Нет снимков"]))

    all_regions = sorted({r[0].strip() for r in rows if r[0].strip()})
    for region in all_regions:
        for year in YEARS:
            if (region, year) not in grouped:
                for month in REQUIRED_MONTHS:
                    date_label = f"{month_names_ru[int(month)]} {year}"
                    full_data.append((region, year, month, [region, date_label, "", "⛔ Нет снимков"]))

    unique_keys = set()
    cleaned = []
    for entry in full_data:
        key = (entry[0], entry[1], entry[2])
        if key not in unique_keys:
            unique_keys.add(key)
            cleaned.append(entry[3])

    def sort_key(r):
        region = r[0]
        parts = r[1].split()
        if len(parts) == 2:
            month_num = month_str_to_number(parts[0]) or "99"
            year = parts[1]
        else:
            month_num = "99"
            year = "9999"
        return (region, year, month_num)

    cleaned.sort(key=sort_key)
    worksheet.clear()
    worksheet.update([headers] + cleaned)
    print("✅ Проверка и дополнение по месяцам завершена.")

def update_sheet(sheets_client):
    try:
        print("Обновление таблицы")
        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        data = worksheet.get_all_values()

        cloud_prob_col = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY')
        s2_sr = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")

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
                days = calendar.monthrange(int(year), int(month_num))[1]
                end_str = f"{year}-{month_num}-{days:02d}"

                print(f"\n{region} — {start} - {end_str}")

                geometry = get_geometry_from_asset(region)

                # Фильтрация коллекции Sentinel-2 SR с маской облаков
                collection = s2_sr \
                    .filterDate(start, end_str) \
                    .filterBounds(geometry) \
                    .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', 40)) \
                    .map(lambda img: 
                         img.updateMask(
                            cloud_prob_col.filter(ee.Filter.eq('system:index', img.get('system:index'))).first()
                            .lt(40)
                         )
                    )

                # Вычисляем валидные пиксели в регионе по одному из каналов (например, B4)
                def add_valid_pixel_count(img):
                    stats = img.select('B4').reduceRegion(
                        reducer=ee.Reducer.count(),
                        geometry=geometry,
                        scale=20,
                        maxPixels=1e9
                    )
                    count = ee.Number(stats.get('B4'))
                    return img.set('valid_pixel_count', count)

                collection = collection.map(add_valid_pixel_count)
                collection = collection.filter(ee.Filter.gt('valid_pixel_count', 0))

                if collection.size().getInfo() == 0:
                    worksheet.update_cell(row_idx, 3, "Нет снимков")
                    continue

                mosaic = collection.mosaic().resample('bilinear').clip(geometry)

                # Исправлено: используем 'mosaic', а не 'filtered_mosaic' (не определена переменная)
                tile_info = ee.data.getMapId( mosaic,
    {
        "bands": ["B4", "B3", "B2"],
        "min": [0, 0, 0],
        "max": [3000, 3000, 3000]
    }
)
                
                mapid = tile_info["mapid"]
                xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{mapid}/tiles/{{z}}/{{x}}/{{y}}"
                worksheet.update_cell(row_idx, 3, xyz)

            except Exception as e:
                log_error(f"Строка {row_idx}", e)
                worksheet.update_cell(row_idx, 3, f"Ошибка: {str(e)[:100]}")

    except Exception as e:
        log_error("update_sheet", e)
        raise

if __name__ == "__main__":
    try:
        client = initialize_services()
        ensure_month_coverage(client)
        update_sheet(client)
        print("Скрипт успешно завершен ✅")
    except Exception as e:
        log_error("main", e)
        exit(1)
