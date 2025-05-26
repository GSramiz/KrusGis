import ee
import json
import os
import traceback
import calendar

# Конфигурация
REGION_NAME = "Белгородская область"
YEAR = 2022
MONTH_NAME = "Май"
ASSET_REGION_PATH = "projects/ee-romantik1994/assets/region"
EXPORT_PATH = "projects/ee-romantik1994/assets/exports"

def log_error(context, error):
    print(f"\nОШИБКА в {context}:")
    print(f"Тип: {type(error).__name__}")
    print(f"Сообщение: {str(error)}")
    traceback.print_exc()
    print("=" * 50)

def month_str_to_number(name):
    months = {
        "Январь": "01", "Февраль": "02", "Март": "03", "Апрель": "04",
        "Май": "05", "Июнь": "06", "Июль": "07", "Август": "08",
        "Сентябрь": "09", "Октябрь": "10", "Ноябрь": "11", "Декабрь": "12"
    }
    return months.get(name.strip().capitalize(), None)

def initialize_gee():
    print("Инициализация Earth Engine...")
    service_account_info = json.loads(os.environ["GEE_CREDENTIALS"])
    credentials = ee.ServiceAccountCredentials(
        service_account_info["client_email"],
        key_data=json.dumps(service_account_info)
    )
    ee.Initialize(credentials)
    print("✅ Earth Engine: инициализирован")

def get_geometry_from_asset(region_name):
    fc = ee.FeatureCollection(ASSET_REGION_PATH)
    region = fc.filter(ee.Filter.eq("title", region_name)).first()
    if region is None:
        raise ValueError(f"Регион '{region_name}' не найден в ассете")
    return region.geometry()

def mask_clouds(img):
    scl = img.select("SCL")
    # Разрешённые классы: vegetation (4), non-vegetated (5), water (6), unclassified (7)
    allowed = scl.eq(4).Or(scl.eq(5)).Or(scl.eq(6)).Or(scl.eq(7))
    return img.updateMask(allowed).resample("bilinear")

def export_belgorod_mosaic():
    try:
        month_num = month_str_to_number(MONTH_NAME)
        if not month_num:
            raise ValueError(f"Неверный месяц: {MONTH_NAME}")

        start = f"{YEAR}-{month_num}-01"
        end_day = calendar.monthrange(YEAR, int(month_num))[1]
        end = f"{YEAR}-{month_num}-{end_day:02d}"

        print(f"Дата: {start} — {end}")

        geometry = get_geometry_from_asset(REGION_NAME)

        collection = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterDate(start, end)
            .filterBounds(geometry)
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 30))
            .map(mask_clouds)
        )

        if collection.size().getInfo() == 0:
            raise RuntimeError("Нет подходящих снимков для мозаики")

        mosaic = collection.mosaic().clip(geometry)

        export_asset_id = f"{EXPORT_PATH}/mosaic_Belgorodskaya_{YEAR}-{month_num}"
        print(f"Экспорт в: {export_asset_id}")

        task = ee.batch.Export.image.toAsset(
            image=mosaic,
            description="Export_Belgorod_Mosaic",
            assetId=export_asset_id,
            region=geometry,
            scale=10,
            maxPixels=1e13
        )
        task.start()
        print("✅ Экспорт запущен")
    except Exception as e:
        log_error("export_belgorod_mosaic", e)
        raise

if __name__ == "__main__":
    try:
        initialize_gee()
        export_belgorod_mosaic()
        print("Скрипт успешно завершён")
    except Exception as e:
        log_error("main", e)
        exit(1)
