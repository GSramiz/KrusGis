import ee
import gspread
import json
import os
import traceback
from oauth2client.service_account import ServiceAccountCredentials

def log_error(context, error):
    print(f"\n❌ ОШИБКА в {context}:")
    print(f"Тип: {type(error).__name__}")
    print(f"Сообщение: {str(error)}")
    traceback.print_exc()
    print("=" * 50)

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
    # Более мягкая маска — игнорируем только толстые облака и тени
    mask = scl.neq(9).And(scl.neq(10))  # exclude clouds and shadows
    return img.updateMask(mask)

def prepare_image(img):
    # Добавляем 'score' для qualityMosaic: -1 * CLOUDY_PIXEL_PERCENTAGE
    score = img.getNumber("CLOUDY_PIXEL_PERCENTAGE").multiply(-1)
    return img.set("score", score)

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
                    .map(lambda img: img.select(["TCI_R", "TCI_G", "TCI_B", "SCL"]).resample("bicubic")) \
                    .map(prepare_image)

                count = collection.size().getInfo()
                print(f"🧮 Изображений после фильтрации: {count}")
                if count == 0:
                    worksheet.update_cell(row_idx, 3, "Нет снимков после фильтрации")
                    continue

                # Пытаемся сначала через qualityMosaic('score')
                try:
                    mosaic = collection.qualityMosaic("score").clip(geometry)
                except Exception as e:
                    print("⚠️ qualityMosaic не сработал, fallback на mosaic()")
                    collection = collection.sort("CLOUDY_PIXEL_PERCENTAGE")
                    mosaic = collection.mosaic().clip(geometry)

                vis = {"bands": ["TCI_R", "TCI_G", "TCI_B"], "min": 0, "max": 255}
                tile_info = ee.data.getMapId({"image": mosaic, "visParams": vis})
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

if __name__ == "__main__":
    try:
        client = initialize_services()
        update_sheet(client)
        print("\n✅ Скрипт успешно завершен")
    except Exception as e:
        log_error("main", e)
        exit(1)
