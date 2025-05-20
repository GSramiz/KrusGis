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

def mask_clouds(img):
    scl = img.select("SCL")
    cloud_mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    return img.updateMask(cloud_mask).resample("bilinear")

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

                size = raw_collection.size().getInfo()
                if size == 0:
                    worksheet.update_cell(row_idx, 3, "Нет снимков")
                    continue

                def add_id_band(img):
                    return img.addBands(
                        ee.Image.constant(0).rename("source_mask").set("system:index", img.get("system:index"))
                    ).set("system:index", img.get("system:index"))

                with_id = raw_collection.map(add_id_band)
                mosaic = with_id.mosaic()

                # Получение system:index из contributing изображений
                contributing_ids = with_id.aggregate_array("system:index").getInfo()
                used_ids = []

                for img_id in contributing_ids:
                    single = with_id.filter(ee.Filter.eq("system:index", img_id)).first()
                    masked = mosaic.mask().And(single.mask()).reduceRegion(
                        reducer=ee.Reducer.anyNonZero(),
                        geometry=geometry,
                        scale=500,
                        maxPixels=1e6
                    )
                    if any(v for v in masked.getInfo().values()):
                        used_ids.append(img_id)

                # Финальная коллекция только из нужных изображений
                final_collection = with_id.filter(ee.Filter.inList("system:index", used_ids))
                final_mosaic = final_collection.mosaic()

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

if __name__ == "__main__":
    try:
        client = initialize_services()
        update_sheet(client)
        print("\n✅ Скрипт успешно завершен")
    except Exception as e:
        log_error("main", e)
        exit(1)
