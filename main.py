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
    # Приводим к Единому Регистру
    return months.get(name.strip().capitalize(), None)

# Кеш геометрий регионов в память, чтобы не делать повторных запросов в Earth Engine
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

# Очищаем облака (без ресемплинга): оставляем только «чистые» пиксели по SCL
def mask_clouds_simple(img):
    scl = img.select("SCL")
    mask = scl.eq(4).Or(scl.eq(5)).Or(scl.eq(6)).Or(scl.eq(7))
    return img.updateMask(mask)

def update_sheet(sheets_client):
    try:
        print("Обновление таблицы...")

        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        data = worksheet.get_all_values()

        # Собираем batch‐список изменений для Google Sheets
        cell_updates = []

        # Проходим по всем строкам, начиная со 2-й (индекс 1), поскольку первая—заголовки
        for row_idx, row in enumerate(data[1:], start=2):
            try:
                # Первая ячейка = название региона
                region = row[0]
                if not region:
                    continue

                # Вторая ячейка (индекс 1) = дата в виде «Месяц Год»
                raw_date = row[1]
                if not raw_date:
                    # Если ячейка пуста, просто пропускаем
                    continue

                # «Страхуемся» на случай, если gspread вернул list вместо строки
                if isinstance(raw_date, (list, tuple)):
                    date_str = " ".join(raw_date)
                else:
                    date_str = str(raw_date)

                parts = date_str.strip().split()
                if len(parts) != 2:
                    raise ValueError(f"Неверный формат даты (ожидается «Месяц Год»): '{date_str}'")

                month_name, year = parts[0], parts[1]
                month_num = month_str_to_number(month_name)
                if month_num is None:
                    raise ValueError(f"Неизвестное название месяца: '{month_name}'")

                # Собираем дату начала (1-го числа) и конца (последний день месяца)
                start = f"{year}-{month_num}-01"
                days = calendar.monthrange(int(year), int(month_num))[1]
                end_str = f"{year}-{month_num}-{days:02d}"

                print(f"\nОбрабатываем {region} — период {start} - {end_str}")

                geometry = get_geometry_from_asset(region)

                collection = (
                    ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                    .filterDate(start, end_str)
                    .filterBounds(geometry)
                    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30))
                    .map(mask_clouds_simple)  # только маска по SCL, без ресемплинга
                )

                # Проверяем, есть ли вообще кадры:
                count = collection.size().getInfo()
                if count == 0:
                    cell_updates.append({
                        'range': f"{SHEET_NAME}!C{row_idx}",
                        'values': [["Нет снимков"]]
                    })
                    continue

                # Делаем мозаичное изображение из «чистых» кадров
                mosaic = collection.mosaic()

                # Опционально: применяем ресемплинг уже к итоговой мозаике
                # (многие обходятся без этого; можно закомментировать, если не критично)
                mosaic = mosaic.resample("bilinear")

                # Получаем MapID для визуализации (B4,B3,B2, диапазон 0–3000)
                tile_info = ee.data.getMapId({
                    "image": mosaic,
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
                # Если что‐то пошло не так — логируем и пишем короткую ошибку в ячейку
                log_error(f"Строка {row_idx}", e)
                short_err = f"Ошибка: {str(e)[:100]}"
                cell_updates.append({
                    'range': f"{SHEET_NAME}!C{row_idx}",
                    'values': [[short_err]]
                })

        # Если есть накопленные обновления — отправляем одним batch‐запросом
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
