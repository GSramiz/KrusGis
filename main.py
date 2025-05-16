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

# Получение оптимального набора снимков для покрытия региона
def get_covering_images(collection, region_geom, max_images=50, threshold=0.95):
    def accumulate_coverage(img_list, covered_area):
        img = ee.Image(img_list[1])
        img_geom = img.geometry().intersection(region_geom, ee.ErrorMargin(1))
        new_covered = covered_area.union(img_geom, ee.ErrorMargin(1))
        return ee.List(img_list[0]).add(img), new_covered

    def iterate(i, acc):
        acc_dict = ee.Dictionary(acc)
        imgs = ee.List(acc_dict.get("imgs"))
        covered = ee.Geometry(acc_dict.get("covered"))
        img = ee.Image(collection.toList(max_images).get(i))
        new_imgs, new_covered = accumulate_coverage([imgs, img], covered)
        return ee.Dictionary({"imgs": new_imgs, "covered": new_covered})

    first = ee.Dictionary({"imgs": ee.List([]), "covered": ee.Geometry.MultiPolygon([])})
    result = ee.Dictionary(ee.List.sequence(0, collection.size().min(max_images).subtract(1)).iterate(
        iterate, first))
    return ee.ImageCollection(result.get("imgs"))

# Обновление Google Sheet
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
                    .map(mask_clouds) \
                    .map(lambda img: img.select(["TCI_R", "TCI_G", "TCI_B"]).resample("bicubic")) \
                    .sort("CLOUDY_PIXEL_PERCENTAGE")

                final_collection = get_covering_images(collection, geometry)
                count = final_collection.size().getInfo()
                if count == 0:
                    worksheet.update_cell(row_idx, 3, "Нет снимков")
                    continue

                mosaic = final_collection.mosaic().clip(geometry)
                vis = {"bands": ["TCI_R", "TCI_G", "TCI_B"], "min": 0, "max": 255}
                tile_info = ee.data.getMapId({"image": mosaic, "visParams": vis})
                clean_mapid = tile_info["mapid"].split("/")[-1]
                xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{clean_mapid}/tiles/{{z}}/{{x}}/{{y}}"

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
