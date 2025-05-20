import ee
import datetime
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ✅ Авторизация
service_account_file = 'auth.json'
credentials = service_account.Credentials.from_service_account_file(
    service_account_file,
    scopes=['https://www.googleapis.com/auth/spreadsheets']
)

# ✅ Инициализация Earth Engine
try:
    ee.Initialize()
except Exception as e:
    ee.Authenticate()
    ee.Initialize()

# ✅ Параметры
SPREADSHEET_ID = '1hZOrnmdzuBAG9JX1NUUnJVVVPt7Md3Or1jxNc_KbApw'
SHEET_NAME = 'Tiles'
DATE_FROM = '2024-06-01'
DATE_TO = '2024-09-01'
MAX_COVERAGE_PERCENT = 95  # Процент покрытия, который хотим достичь

# ✅ Сервис Google Sheets
def get_sheets_service():
    return build('sheets', 'v4', credentials=credentials).spreadsheets()

def update_sheet(service, values):
    body = {'values': values}
    service.values().clear(spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A2:B1000').execute()
    service.values().update(spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A2', valueInputOption='RAW', body=body).execute()

# ✅ Чтение тайлов из таблицы
sheet_service = get_sheets_service()
tile_geometries = sheet_service.values().get(spreadsheetId=SPREADSHEET_ID, range=f'{SHEET_NAME}!A2:A').execute().get('values', [])
tile_geometries = [ee.Geometry.Rectangle(eval(row[0])) for row in tile_geometries if row]

# ✅ Маскирование облаков по SCL
CLOUDY = [3, 7, 8, 9, 10, 11]
def mask_scl(image):
    scl = image.select('SCL')
    mask = ~scl.isin(CLOUDY)
    return image.updateMask(mask)

# ✅ Обработка тайла
def process_tile(geom):
    region_area = geom.area()

    collection = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')\
        .filterDate(DATE_FROM, DATE_TO)\
        .filterBounds(geom)\
        .map(mask_scl)\
        .sort('CLOUDY_PIXEL_PERCENTAGE')

    def add_mask(image):
        return image.set('mask_area', image.select('B8').mask().reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geom,
            scale=20,
            maxPixels=1e8
        ).get('B8'))

    images_with_mask = collection.map(add_mask)
    image_list = images_with_mask.toList(images_with_mask.size())

    def get_selected():
        coverage = ee.Image(0)
        total_area = ee.Number(0)
        selected = []
        i = 0
        while i < image_list.size().getInfo():
            img = ee.Image(image_list.get(i))
            mask = img.select('B8').mask()
            coverage = coverage.unmask(0).Or(mask)

            current_area = coverage.reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=geom,
                scale=20,
                maxPixels=1e8
            ).getNumber('B8')

            percent = current_area.multiply(20*20).divide(region_area).multiply(100)
            selected.append(img)
            if percent.gte(MAX_COVERAGE_PERCENT):
                break
            i += 1
        return selected

    selected_images = get_selected()

    if not selected_images:
        return ['Нет снимков']

    mosaic = ee.ImageCollection.fromImages(selected_images).mosaic()
    vis_params = {
        'bands': ['TCI_R', 'TCI_G', 'TCI_B'],
        'min': 0,
        'max': 3000,
        'format': 'jpg'
    }
    tile_info = ee.data.getMapId({'image': mosaic.visualize(**vis_params), 'format': 'jpg', 'resampling': 'bilinear'})
    raw_mapid = tile_info['tile_fetcher'].url_format
    clean_mapid = raw_mapid.split("/")[-2]
    xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{clean_mapid}/tiles/{{z}}/{{x}}/{{y}}"
    return [str(geom.bounds().coordinates().getInfo()), xyz]

# ✅ Основной цикл
results = []
for tile_geom in tile_geometries:
    try:
        row = process_tile(tile_geom)
        results.append(row)
    except Exception as e:
        results.append([str(tile_geom.bounds().coordinates().getInfo()), f"Ошибка: {e}"])

update_sheet(sheet_service, results)
print("✅ Готово")
