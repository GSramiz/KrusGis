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
    geom = region.geometry()
    if geom is None:
        raise ValueError(f"Геометрия региона '{region_name}' не найдена")
    return geom

# Маскирование облаков по SCL
def mask_clouds(img):
    scl = img.select("SCL")
    cloud_mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    return img.updateMask(cloud_mask)

# Подсчет чистой площади снимка
def get_valid_area(img, geom):
    scl = img.select("SCL")
    valid_mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    count = valid_mask.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geom,
        scale=20,
        maxPixels=1e9
    ).get("SCL")
    pixel_area = ee.Number(400)  # 20м x 20м = 400 м²
    return ee.Number(count).multiply(pixel_area)

# Сбор минимального набора снимков
def get_minimum_mosaic(collection, geom, threshold=0.95):
    total_area = ee.Number(
        ee.Image.pixelArea().reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=20,
            maxPixels=2e9
        ).get("area")
    )

    def iterate_function(img, state):
        img = ee.Image(img)
        state = ee.Dictionary(state)
        current_area = ee.Number(state.get("current_area"))
        images = ee.List(state.get("images"))
        new_area = get_valid_area(img, geom)
        total = current_area.add(new_area)
        images = images.add(img)
        return ee.Algorithms.If(
            total.divide(total_area).lt(threshold),
            ee.Dictionary({"current_area": total, "images": images}),
            state
        )

    initial_state = ee.Dictionary({"current_area": 0, "images": ee.List([])})
    final_state = ee.List(collection.toList(collection.size())).iterate(iterate_function, initial_state)
    result_list = ee.Dictionary(final_state).get("images")
    return ee.ImageCollection(ee.List(result_list))

# Основная функция
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

                collection = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED") \
                    .filterDate(start, end) \
                    .filterBounds(geometry) \
                    .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 40)) \
                    .sort("CLOUDY_PIXEL_PERCENTAGE") \
                    .map(mask_clouds)

                count = collection.size().getInfo()
                if count == 0:
                    worksheet.update_cell(row_idx, 3, "Нет снимков")
                    continue

                print(f"🧩 Найдено {count} снимков, формируем мозаику...")

                best_subset = get_minimum_mosaic(collection, geometry, threshold=0.95)
                subset_count = best_subset.size().getInfo()

                if subset_count == 0:
                    worksheet.update_cell(row_idx, 3, "Недостаточно покрытие (<95%)")
                    continue

                best_subset = best_subset.map(lambda img: img.resample("bicubic"))
                mosaic = best_subset.mosaic().clip(geometry)

                vis = {"bands": ["TCI_R", "TCI_G", "TCI_B"], "min": 0, "max": 255}
                visualized = mosaic.visualize(**vis)

                tile_info = ee.data.getMapId({"image": visualized})
                mapid = tile_info["mapid"].split("/")[-1]
                xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{mapid}/tiles/{{z}}/{{x}}/{{y}}"

                worksheet.update_cell(row_idx, 3, xyz)
                worksheet.update_cell(row_idx, 4, f"{subset_count} снимков")

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
