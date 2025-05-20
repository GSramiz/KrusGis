import ee
import gspread
import json
import os
import traceback
import calendar
from oauth2client.service_account import ServiceAccountCredentials

def log_error(context, error):
    print(f"\n❌ ОШИБКА в {context}:")
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

# ✅ Маска облаков
def mask_clouds(img):
    scl = img.select("SCL")
    cloud_mask = scl.neq(3).And(scl.neq(7)).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10)).And(scl.neq(11))
    return img.updateMask(cloud_mask).resample("bilinear")

# ✅ Построение мозаики с порогом покрытия
def build_mosaic(geometry, start_date, end_date, max_coverage_percent=95):
    region_area = geometry.area()

    collection = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED") \
        .filterDate(start_date, end_date) \
        .filterBounds(geometry) \
        .map(mask_clouds) \
        .sort("CLOUDY_PIXEL_PERCENTAGE")

    def add_mask(image):
        return image.set("mask_area", image.select("B8").mask().reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geometry,
            scale=20,
            maxPixels=1e8
        ).get("B8"))

    images_with_mask = collection.map(add_mask)
    image_list = images_with_mask.toList(images_with_mask.size())

    coverage = ee.Image(0)
    selected = []
    i = 0
    while True:
        try:
            img = ee.Image(image_list.get(i))
        except:
            break
        mask = img.select("B8").mask()
        coverage = coverage.unmask(0).Or(mask)

        current_area = coverage.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geometry,
            scale=20,
            maxPixels=1e8
        ).getNumber("B8")

        percent = current_area.multiply(400).divide(region_area).multiply(100)
        selected.append(img)

        if percent.gte(max_coverage_percent):
            break
        i += 1
        if i >= image_list.size().getInfo():
            break

    if not selected:
        return None

    mosaic = ee.ImageCollection.fromImages(selected).mosaic()
    vis = {"bands": ["TCI_R", "TCI_G", "TCI_B"], "min": 0, "max": 3000}
    visualized = mosaic.visualize(**vis)

    tile_info = ee.data.getMapId({"image": visualized})
    clean_mapid = tile_info["mapid"]
    return f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{clean_mapid}/tiles/{{z}}/{{x}}/{{y}}"

# ✅ Обновление таблицы
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
                days = calendar.monthrange(int(year), int(month_num))[1]
                end = f"{year}-{month_num}-{days:02d}"

                print(f"\n🌍 {region} — {start} - {end}")
                geometry = get_geometry_from_asset(region)

                xyz = build_mosaic(geometry, start, end)
                if not xyz:
                    worksheet.update_cell(row_idx, 3, "Нет снимков")
                    continue

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
        update_sheet(client)
        print("\n✅ Скрипт успешно завершен")
    except Exception as e:
        log_error("main", e)
        exit(1)
