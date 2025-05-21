import ee
import os
import gspread
import calendar
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

# Инициализация Earth Engine
service_account = os.environ.get("EE_SERVICE_ACCOUNT")
credentials = ee.ServiceAccountCredentials(service_account, os.environ.get("EE_PRIVATE_KEY"))
ee.Initialize(credentials)

# Авторизация Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
gs_credentials = ServiceAccountCredentials.from_json_keyfile_name(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"), scope)
gs_client = gspread.authorize(gs_credentials)

def log_error(context, e):
    print(f"\n❌ Ошибка в {context}: {str(e)}")

def month_str_to_number(month_str):
    months = {
        "январь": "01", "февраль": "02", "март": "03", "апрель": "04",
        "май": "05", "июнь": "06", "июль": "07", "август": "08",
        "сентябрь": "09", "октябрь": "10", "ноябрь": "11", "декабрь": "12"
    }
    return months.get(month_str.lower(), "01")

def get_geometry_from_asset(region):
    asset_path = f"users/your_username/regions/{region}"
    return ee.FeatureCollection(asset_path).geometry()

def mask_clouds(img):
    scl = img.select("SCL")
    cloud_mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    return img.updateMask(cloud_mask)

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
                end_str = f"{year}-{month_num}-{days:02d}"

                print(f"\n🌍 {region} — {start} - {end_str}")

                geometry = get_geometry_from_asset(region)

                raw_collection = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED") \
                    .filterDate(start, end_str) \
                    .filterBounds(geometry) \
                    .map(mask_clouds)

                # Добавляем уникальный 'source' слой по system:index
                def add_source_band(img):
                    return img.addBands(
                        ee.Image.constant(1).rename("source")
                        .updateMask(img.mask().reduce(ee.Reducer.min()))
                    ).set("system:index", img.get("system:index"))

                with_source = raw_collection.map(add_source_band)

                # Строим мозаику из слоя 'source'
                source_mosaic = with_source.select("source").mosaic()

                # Получаем ID снимков, у которых хоть один пиксель остался в мозаике
                def was_used(img):
                    source_mask = img.select("source")
                    overlap = source_mosaic.And(source_mask)
                    any_overlap = overlap.reduceRegion(
                        reducer=ee.Reducer.anyNonZero(),
                        geometry=geometry,
                        scale=1000,
                        maxPixels=1e6
                    )
                    return ee.Feature(None, {
                        "system:index": img.get("system:index"),
                        "used": any_overlap.values().contains(True)
                    })

                used_features = with_source.map(was_used).filter(ee.Filter.eq("used", True))
                used_ids = used_features.aggregate_array("system:index")
                filtered_collection = with_source.filter(ee.Filter.inList("system:index", used_ids))

                size = filtered_collection.size().getInfo()
                if size == 0:
                    worksheet.update_cell(row_idx, 3, "Нет снимков")
                    continue

                final_mosaic = filtered_collection.mosaic()

                vis = {"bands": ["B4", "B3", "B2"], "min": 0, "max": 3000}
                visualized = final_mosaic.select(["B4", "B3", "B2"]).visualize(**vis)

                tile_info = ee.data.getMapId({"image": visualized})
                clean_mapid = tile_info["mapid"].split("/")[-1]
                xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{clean_mapid}/tiles/{{z}}/{{x}}/{{y}}"

                worksheet.update_cell(row_idx, 3, xyz)

            except Exception as e:
                log_error(f"Строка {row_idx}", e)
                worksheet.update_cell(row_idx, 3, f"Ошибка: {str(e)[:100]}")

    except Exception as e:
        log_error("update_sheet", e)
        raise

# Запуск
update_sheet(gs_client)
