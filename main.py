import ee
import gspread
import json
import os
import traceback
import calendar
from oauth2client.service_account import ServiceAccountCredentials

# Логирование ошибок
def log_error(context, error):
    print(f"\nОШИБКА в {context}:")
    print(f"Тип: {type(error).__name__}")
    print(f"Сообщение: {str(error)}")
    traceback.print_exc()
    print("=" * 50)

# Инициализация Earth Engine и Google Sheets
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

# Генерация тайлов по геометрии
def generate_tiles(geometry, tile_size_deg=0.25):
    bounds = geometry.bounds().coordinates().get(0)
    coords = ee.List(bounds).map(lambda c: ee.List(c))
    lon_min = ee.Number(ee.List(coords.get(0)).get(0))
    lat_min = ee.Number(ee.List(coords.get(0)).get(1))
    lon_max = ee.Number(ee.List(coords.get(2)).get(0))
    lat_max = ee.Number(ee.List(coords.get(2)).get(1))

    lons = ee.List.sequence(lon_min, lon_max, tile_size_deg)
    lats = ee.List.sequence(lat_min, lat_max, tile_size_deg)

    def create_tile(lat):
        def create_lon_tile(lon):
            return ee.Feature(ee.Geometry.Rectangle([lon, lat, ee.Number(lon).add(tile_size_deg), ee.Number(lat).add(tile_size_deg)]))
        return lons.map(create_lon_tile)

    tiles_nested = lats.map(create_tile)
    tiles_flat = tiles_nested.flatten()
    return ee.FeatureCollection(tiles_flat).filterBounds(geometry)

# Основная логика обновления таблицы
def update_sheet(sheets_client):
    try:
        print("\nОбновление таблицы")

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
                end_str = f"{year}-{month_num}-{days:02d}"

                print(f"\n{region} — {start} - {end_str}")

                geometry = get_geometry_from_asset(region)
                tiles = generate_tiles(geometry)

                def process_tile(tile):
                    geom = tile.geometry()
                    img = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED") \
                        .filterDate(start, end_str) \
                        .filterBounds(geom) \
                        .map(mask_clouds) \
                        .map(lambda img: img.resample("bicubic")) \
                        .mosaic().clip(geom)

                    stats = img.reduceRegion(
                        reducer=ee.Reducer.count(),
                        geometry=geom,
                        scale=10,
                        maxPixels=1e9
                    )

                    pixel_count = ee.Number(stats.get("TCI_R"))
                    return ee.Feature(tile).set({"img": img, "pixel_count": pixel_count})

                processed = tiles.map(process_tile)
                images = processed.aggregate_array("img")
                counts = processed.aggregate_array("pixel_count")

                total = ee.List(counts).reduce(ee.Reducer.sum())
                total_val = total.getInfo()
                if total_val == 0:
                    worksheet.update_cell(row_idx, 3, "Нет снимков")
                    continue

                mosaic = ee.ImageCollection(images).mosaic().clip(geometry)
                vis = {"bands": ["TCI_R", "TCI_G", "TCI_B"], "min": 0, "max": 255}
                visualized = mosaic.select(["TCI_R", "TCI_G", "TCI_B"]).visualize(**vis)
                tile_info = ee.data.getMapId({"image": visualized})
                raw_mapid = tile_info["mapid"]
                clean_mapid = raw_mapid.split("/")[-1]
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
        print("\nСкрипт успешно завершен")
    except Exception as e:
        log_error("main", e)
        exit(1)
