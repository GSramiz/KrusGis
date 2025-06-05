import ee
import gspread
import json
import os
import traceback
import calendar
from oauth2client.service_account import ServiceAccountCredentials

# Конфигурация
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY")
SHEET_NAME = "Sentinel-2 Покрытие"

def log_error(context, error):
    print(f"\nОШИБКА в {context}:")
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

def month_str_to_number(name):
    months = {
        "Январь": "01", "Февраль": "02", "Март": "03", "Апрель": "04",
        "Май": "05", "Июнь": "06", "Июль": "07", "Август": "08",
        "Сентябрь": "09", "Октябрь": "10", "Ноябрь": "11", "Декабрь": "12"
    }
    return months.get(name.strip().capitalize(), None)

# Кеш геометрий регионов (чтобы не скачивать несколько раз одну и ту же геометрию)
_region_cache = {}
def get_geometry_from_asset(region_name):
    if region_name in _region_cache:
        return _region_cache[region_name]
    fc = ee.FeatureCollection("projects/ee-romantik1994/assets/region")
    region = fc.filter(ee.Filter.eq("title", region_name)).first()
    if region is None:
        raise ValueError(f"Регион '{region_name}' не найден в ассете")
    geom = region.geometry()
    _region_cache[region_name] = geom
    return geom

# Маска облаков (без ресемплинга)
def mask_clouds(img):
    scl = img.select("SCL")
    # Оставляем только «чистые» пиксели: 4,5,6,7
    allowed = scl.eq(4).Or(scl.eq(5)).Or(scl.eq(6)).Or(scl.eq(7))
    return img.updateMask(allowed)

def update_sheet(sheets_client):
    try:
        print("Обновление таблицы...")

        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        data = worksheet.get_all_values()

        # Собираем batch-список изменений
        cell_updates = []

        # Проходим по строкам, начиная со 2-й (заголовки в 1-й)
        for row_idx, row in enumerate(data[1:], start=2):
            try:
                # В точности как в референсе:
                region, date_str = row[:2]
                if not region or not date_str:
                    continue

                # Если date_str неожиданно list или tuple, склеиваем в строку
                if isinstance(date_str, (list, tuple)):
                    date_str = " ".join(date_str)
                else:
                    date_str = str(date_str)

                parts = date_str.strip().split()
                if len(parts) != 2:
                    raise ValueError(f"Неверный формат даты: '{date_str}'")

                month_num = month_str_to_number(parts[0])
                year = parts[1]
                if month_num is None:
                    raise ValueError(f"Неизвестное название месяца: '{parts[0]}'")

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
                    .map(mask_clouds)  # маска облаков, без ресемплинга
                )

                # Быстрая проверка наличия снимков через size()
                count = collection.size().getInfo()
                if count == 0:
                    cell_updates.append({
                        'range': f"{SHEET_NAME}!C{row_idx}",
                        'values': [["Нет снимков"]]
                    })
                    continue

                # Собираем мозаику из «чистых» кадров
                filtered_mosaic = collection.mosaic()

                # Ресемплинг уже к итоговой мозаике (опционально)
                filtered_mosaic = filtered_mosaic.resample("bilinear")

                tile_info = ee.data.getMapId({
                    "image": filtered_mosaic,
                    "bands": ["B4", "B3", "B2"],
                    "min": [0, 0, 0],
                    "max": [3000, 3000, 3000]
                })

                clean_mapid = tile_info["mapid"].split("/")[-1]
                xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{clean_mapid}/tiles/{{z}}/{{x}}/{{y}}"

                cell_updates.append({
                    'range': f"{SHEET_NAME}!C{row_idx}",
                    'values': [[xyz]]
                })

            except Exception as e:
                log_error(f"Строка {row_idx}", e)
                short_err = f"Ошибка: {str(e)[:100]}"
                cell_updates.append({
                    'range': f"{SHEET_NAME}!C{row_idx}",
                    'values': [[short_err]]
                })

        # Отправляем все обновления одним batch_update
        if cell_updates:
            body = {
                'valueInputOption': "USER_ENTERED",
                'data': cell_updates
            }
            worksheet.spreadsheet.batch_update(body)

        print("Обновление завершено.")

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
