# main.py (обновлённый)
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

        if "GEE_CREDENTIALS" not in os.environ:
            raise ValueError("Отсутствует переменная GEE_CREDENTIALS")

        service_account_info = json.loads(os.environ["GEE_CREDENTIALS"])

        credentials = ee.ServiceAccountCredentials(
            service_account_info['client_email'],
            key_data=json.dumps(service_account_info)
        )
        ee.Initialize(credentials)
        print("✅ Earth Engine: успешная инициализация")

        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        sheets_client = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
        )
        print("✅ Google Sheets: успешная авторизация")
        return sheets_client

    except Exception as e:
        log_error("initialize_services", e)
        raise


def update_sheet(sheets_client):
    try:
        print("\n📊 Начало обновления таблицы")

        SPREADSHEET_ID = "1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY"
        SHEET_NAME = "Sentinel-2 Покрытие"

        month_map = {
            "Январь": "01", "Февраль": "02", "Март": "03", "Апрель": "04",
            "Май": "05", "Июнь": "06", "Июль": "07", "Август": "08",
            "Сентябрь": "09", "Октябрь": "10", "Ноябрь": "11", "Декабрь": "12"
        }

        sheet = sheets_client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
        data = sheet.get_all_values()

        for row_idx, row in enumerate(data[1:], start=2):
            region_name, period = row[:2]

            try:
                month_rus, year = period.strip().split()
                month = month_map.get(month_rus.capitalize())
                if not month:
                    raise ValueError(f"Неизвестный месяц: {month_rus}")

                start = f"{year}-{month}-01"
                end = ee.Date(start).advance(1, 'month')

                region_geometry = get_region_geometry(region_name)
                if region_geometry is None:
                    raise ValueError("Геометрия региона не найдена")

                mosaic = create_smoothed_mosaic(start, end, region_geometry)

                vis = {
                    'bands': ['TCI_R', 'TCI_G', 'TCI_B'],
                    'min': 0,
                    'max': 255
                }
                map_info = mosaic.visualize(**vis).getMap()
                xyz = f"https://earthengine.googleapis.com/map/{map_info['mapid']}/%7Bz%7D/%7Bx%7D/%7By%7D?token={map_info['token']}"

                sheet.update_cell(row_idx, 3, xyz)

            except Exception as e:
                log_error(f"обработка строки {row_idx}", e)
                sheet.update_cell(row_idx, 3, f"Ошибка: {str(e)[:100]}")

        print("\n✅ Обновление завершено")

    except Exception as e:
        log_error("update_sheet", e)


def get_region_geometry(name):
    fc_gaul = ee.FeatureCollection("FAO/GAUL/2015/level1")
    fc_alt = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017")
    
    region = fc_gaul.filter(ee.Filter.eq('ADM1_NAME', name)).geometry()
    alt_region = fc_alt.filter(ee.Filter.eq('country_na', name)).geometry()

    return ee.Algorithms.If(region.isDefined(), region, alt_region)


def create_smoothed_mosaic(start_date, end_date, region):
    vis_bands = ['TCI_R', 'TCI_G', 'TCI_B']

    def mask_clouds(img):
        scl = img.select("SCL")
        cloud_classes = ee.List([3, 8, 9, 10])
        mask = scl.remap(cloud_classes, ee.List.repeat(0, cloud_classes.length()), 1)
        return img.updateMask(mask)

    collection = ee.ImageCollection("COPERNICUS/S2_SR")\
        .filterDate(start_date, end_date)\
        .filterBounds(region)\
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 60))\
        .map(mask_clouds)\
        .map(lambda img: img.select(vis_bands).resample('bicubic').copyProperties(img, img.propertyNames()))

    mosaic = collection.mosaic().clip(region)
    kernel = ee.Kernel.gaussian(radius=1.2, sigma=1.2, units='pixels', normalize=True)
    return mosaic.convolve(kernel)


if __name__ == "__main__":
    try:
        print("\n🚀 Запуск скрипта")
        client = initialize_services()
        if client:
            update_sheet(client)
        print("\n🏁 Скрипт завершён успешно")
    except Exception as e:
        log_error("main", e)
        print("\n💥 Критическая ошибка")
