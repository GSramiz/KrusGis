import ee
import os
import json
import datetime
import gspread

# Авторизация Earth Engine через переменную окружения GEE_CREDENTIALS
service_account_info = json.loads(os.environ["GEE_CREDENTIALS"])
credentials = ee.ServiceAccountCredentials(
    service_account_info["client_email"],
    key_data=json.dumps(service_account_info)
)
ee.Initialize(credentials)

# Авторизация Google Sheets
sheets_client = gspread.service_account_from_dict(service_account_info)
sheet = sheets_client.open_by_url(os.environ["GSHEET_URL"]).sheet1

# Настройки
region = ee.Geometry.Polygon(json.loads(os.environ["REGION_COORDS"]))
start_date = os.environ["START_DATE"]
end_date = os.environ["END_DATE"]
tile_scale = 4

# Получение коллекции Sentinel-2 и маскирование облаков по SCL
collection = (
    ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
    .filterBounds(region)
    .filterDate(start_date, end_date)
    .map(lambda img: img.updateMask(img.select('SCL').neq(3)  # облака
                                     .And(img.select('SCL').neq(8))  # облака
                                     .And(img.select('SCL').neq(9))  # тень
                                     .And(img.select('SCL').neq(10))  # облака
                                     .And(img.select('SCL').neq(1))))  # saturated
)

# Сортировка по дате и сборка мозаики
collection = collection.sort('system:time_start')
mosaic = collection.mosaic().clip(region)

# Определение снимков, которые реально видимы в мозайке
visible_ids = collection
    .map(lambda img: img.set('visible', mosaic.eq(img).reduceRegion(
        reducer=ee.Reducer.anyNonZero(),
        geometry=region,
        scale=10,
        maxPixels=1e8
    ).values().contains(True)))
    .filter(ee.Filter.eq('visible', True))
    .aggregate_array('system:index')

# Отбор только реально отображающихся снимков
filtered_collection = collection.filter(ee.Filter.inList('system:index', visible_ids))

# Финальная мозаика только из видимых снимков
final_mosaic = filtered_collection.mosaic().clip(region)

# Генерация XYZ-ссылки
url = final_mosaic.visualize(min=0, max=3000, bands=['TCI_R', 'TCI_G', 'TCI_B']) \
    .getMapId()['tile_fetcher'].url_format

# Загрузка ссылки в Google Sheets
sheet.append_row([str(datetime.datetime.now()), url])
