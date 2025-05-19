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

# Перевод месяца
def month_str_to_number(name):
    months = {
        "Январь": "01", "Февраль": "02", "Март": "03", "Апрель": "04",
        "Май": "05", "Июнь": "06", "Июль": "07", "Август": "08",
        "Сентябрь": "09", "Октябрь": "10", "Ноябрь": "11", "Декабрь": "12"
    }
    return months.get(name.strip().capitalize(), None)

# Геометрия региона
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

# Генерация тайлов 0.25° x 0.25°
def generate_tiles(geometry, tile_size_deg=0.25):
    bounds = geometry.bounds().coordinates().get(0)
    coords = ee.List(bounds)
    lons = coords.map(lambda c: ee.List(c).get(0))
    lats = coords.map(lambda c: ee.List(c).get(1))
    xmin = ee.Number(lons.reduce(ee.Reducer.min()))
    xmax = ee.Number(lons.reduce(ee.Reducer.max()))
    ymin = ee.Number(lats.reduce(ee.Reducer.min()))
    ymax = ee.Number(lats.reduce(ee.Reducer.max()))

    tiles = []
    lat = ymin
    while lat.lt(ymax):
        lon = xmin
        while lon.lt(xmax):
            tile = ee.Geometry.Rectangle(
                [lon, lat, lon.add(tile_size_deg), lat.add(tile_size_deg)]
            ).intersection(geometry, 1)
            tiles.append(tile)
            lon = lon.add(tile_size_deg)
        lat = lat.add(tile_size_deg)

    return ee.List(tiles)

# Сборка мозаики по тайлам
def build_mosaic_from_tiles(geometry, start, end):
    tiles = generate_tiles(geometry)

    def process_tile(tile):
        tile_geom = ee.Geometry(tile)
        collection = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED") \
            .filterDate(start, end) \
            .filterBounds(tile_geom) \
            .map(mask_clouds)

        return collection.mosaic().clip(tile_geom)

    mosaics = tiles.map(process_tile)
    return ee.ImageCollection(mosaics).mosaic().clip(geometry)

# Основная логика

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
                end = f"{year}-{month_num}-28"  # Условный конец месяца

                print(f"\n🌍 {region} — {start} - {end}")

                geometry = get_geometry_from_asset(region)
                collection = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED") \
                    .filterDate(start, end) \
                    .filterBounds(geometry)

                if collection.size().getInfo() == 0:
                    worksheet.update_cell(row_idx, 3, "Нет снимков")
                    continue

                mosaic = build_mosaic_from_tiles(geometry, start, end)
                rgb = mosaic.select(["TCI_R", "TCI_G", "TCI_B"]).divide(255).multiply(255).uint8()
                tile_info = ee.data.getMapId({"image": rgb.clip(geometry)})
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
