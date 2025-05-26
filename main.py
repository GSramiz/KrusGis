import ee
import json
import os
import calendar

# Настройки региона и даты
REGION_TITLE = "Белгородская область"
YEAR = 2022
MONTH_NAME = "Май"

# Загрузка и инициализация Earth Engine
service_account_info = json.loads(os.environ["GEE_CREDENTIALS"])
credentials = ee.ServiceAccountCredentials(
    service_account_info["client_email"],
    key_data=json.dumps(service_account_info)
)
ee.Initialize(credentials)
print("✅ Earth Engine инициализирован")

# Преобразуем название месяца в номер
def month_str_to_number(name):
    months = {
        "Январь": "01", "Февраль": "02", "Март": "03", "Апрель": "04",
        "Май": "05", "Июнь": "06", "Июль": "07", "Август": "08",
        "Сентябрь": "09", "Октябрь": "10", "Ноябрь": "11", "Декабрь": "12"
    }
    return months.get(name.strip().capitalize(), None)

month_num = month_str_to_number(MONTH_NAME)
start_date = f"{YEAR}-{month_num}-01"
end_date = f"{YEAR}-{month_num}-{calendar.monthrange(YEAR, int(month_num))[1]:02d}"

# Получение геометрии региона из ассета
region_fc = ee.FeatureCollection("projects/ee-romantik1994/assets/region")
region = region_fc.filter(ee.Filter.eq("title", REGION_TITLE)).first()
geometry = region.geometry()

# Маска облаков по SCL
def mask_clouds(img):
    scl = img.select("SCL")
    clear = scl.eq(4).Or(scl.eq(5)).Or(scl.eq(6)).Or(scl.eq(7))
    return img.updateMask(clear).resample("bilinear")

# Получение коллекции и построение мозаики
collection = (
    ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
    .filterDate(start_date, end_date)
    .filterBounds(geometry)
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30))
    .map(mask_clouds)
)

if collection.size().getInfo() == 0:
    print("⚠️ Нет снимков для выбранных параметров.")
    exit()

mosaic = collection.mosaic().clip(geometry)

# Экспорт в ассеты (без подпапки exports)
EXPORT_ID = f"mosaic_Belgorodskaya_{YEAR}-{month_num}"
EXPORT_PATH = f"projects/ee-romantik1994/assets/{EXPORT_ID}"

task = ee.batch.Export.image.toAsset(
    image=mosaic,
    description="export_mosaic_belgorod",
    assetId=EXPORT_PATH,
    region=geometry.bounds().getInfo()["coordinates"],
    scale=10,
    maxPixels=1e13
)

task.start()
print(f"✅ Экспорт запущен: {EXPORT_PATH}")
