import ee
import datetime
import os
import io
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# Инициализация Earth Engine и Google API
SERVICE_ACCOUNT = 'gee-script@ee-romantik1994.iam.gserviceaccount.com'
FOLDER_ID = '1IAAEI0NDp_X5iy78jmGPzwJcF6POykRd'
SPREADSHEET_ID = '1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY'
ee.Initialize(project='ee-romantik1994', credentials=ee.ServiceAccountCredentials(SERVICE_ACCOUNT))

# Авторизация Google Sheets и Drive
creds = Credentials.from_service_account_file('credentials.json', scopes=[
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
])
service_sheets = build('sheets', 'v4', credentials=creds)
service_drive = build('drive', 'v3', credentials=creds)
gs = gspread.authorize(creds)
sheet = gs.open_by_key(SPREADSHEET_ID).sheet1

# Настройки
REGIONS = [f['properties']['title'] for f in ee.FeatureCollection('projects/ee-romantik1994/assets/region').getInfo()['features']]
YEARS = [2022, 2023, 2024, 2025]
MONTHS = ['Май', 'Июнь', 'Июль', 'Август', 'Сентябрь']
MONTH_NUM = {'Май': 5, 'Июнь': 6, 'Июль': 7, 'Август': 8, 'Сентябрь': 9}

# Удаление старых данных в таблице
sheet.resize(rows=1)
sheet.update('A1:C1', [['Регион', 'Месяц и год', 'URL покрытия (авто)']])

# Создание QLR файла
def create_qlr(region, month, year, xyz_url):
    qlr = f"""<?xml version='1.0' encoding='UTF-8'?>
<qgis projectname="{region}_{month}_{year}">
  <layer-tree-group>
    <layer-tree-layer id="xyz_{region}_{month}_{year}" name="Sentinel-2: {region} {month} {year}" providerKey="wms"/>
  </layer-tree-group>
  <layer-tree-layer id="xyz_{region}_{month}_{year}" name="Sentinel-2: {region} {month} {year}" providerKey="wms"/>
  <layer id="xyz_{region}_{month}_{year}" name="Sentinel-2: {region} {month} {year}" type="xyz">
    <id>xyz_{region}_{month}_{year}</id>
    <datasource>{xyz_url}</datasource>
  </layer>
</qgis>"""
    return qlr

# Загрузка файла в Google Drive и возврат публичной ссылки
def upload_to_drive(filename, content):
    file_metadata = {
        'name': filename,
        'parents': [FOLDER_ID],
        'mimeType': 'application/xml'
    }
    media = MediaIoBaseUpload(io.BytesIO(content.encode()), mimetype='application/xml')
    file = service_drive.files().create(body=file_metadata, media_body=media, fields='id').execute()
    file_id = file.get('id')
    service_drive.permissions().create(fileId=file_id, body={'role': 'reader', 'type': 'anyone'}).execute()
    return f'https://drive.google.com/uc?id={file_id}&export=download'

# Обработка по регионам и датам
for region in REGIONS:
    region_fc = ee.FeatureCollection('projects/ee-romantik1994/assets/region').filter(ee.Filter.eq('title', region))
    geometry = region_fc.geometry()
    for year in YEARS:
        for month in MONTHS:
            if year == 2025 and MONTH_NUM[month] > 5:
                continue  # Не обрабатывать месяцы после мая 2025
            start = ee.Date.fromYMD(year, MONTH_NUM[month], 1)
            end = start.advance(1, 'month')

            collection = ee.ImageCollection('COPERNICUS/S2_HARMONIZED')\
                .filterDate(start, end)\
                .filterBounds(geometry)\
                .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 50))

            image = collection.sort('CLOUDY_PIXEL_PERCENTAGE').mosaic()
            scl = image.select('SCL')
            mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
            image_masked = image.updateMask(mask).select(['TCI_R', 'TCI_G', 'TCI_B'])
            vis_params = {'min': 0, 'max': 3000, 'bands': ['TCI_R', 'TCI_G', 'TCI_B']}
            image_smoothed = image_masked.resample('bicubic').convolve(ee.Kernel.gaussian(2))

            try:
                map_id_dict = ee.data.getMapId({'image': image_smoothed.visualize(**vis_params)})
                mapid = map_id_dict['mapid']
                token = map_id_dict['token']
                url = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{mapid}/tiles/{{z}}/{{x}}/{{y}}?token={token}"

                qlr_text = create_qlr(region, month, year, url)
                filename = f"{region}_{month}_{year}.qlr"
                qlr_url = upload_to_drive(filename, qlr_text)

                sheet.append_row([region, f"{month} {year}", qlr_url])
            except Exception:
                sheet.append_row([region, f"{month} {year}", 'Нет снимков'])
