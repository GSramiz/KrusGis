# main.py
import os
import ee
import gspread
from datetime import datetime
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from utils.auth import get_ee_service
from utils.date_utils import parse_month_year, is_after_may_2025
from utils.qlr_exporter import generate_qlr_file, upload_to_drive

# Константы
SHEET_ID = '1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY'
SHEET_NAME = 'Sentinel-2 Покрытие'
DRIVE_FOLDER_ID = '1IAAEI0NDp_X5iy78jmGPzwJcF6POykRd'
REGIONS_ASSET = 'projects/ee-romantik1994/assets/region'
ACCOUNT_EMAIL = 'gee-script@ee-romantik1994.iam.gserviceaccount.com'

# Авторизация через файл
service_account_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'service-account.json')

credentials = Credentials.from_service_account_file(service_account_path, scopes=[
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets'])

gs_client = gspread.authorize(credentials)
ee.Initialize(credentials.with_subject(ACCOUNT_EMAIL))

# Таблица
sheet = gs_client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)

# Загрузка всех названий регионов
regions = ee.FeatureCollection(REGIONS_ASSET).aggregate_array('title').getInfo()
data = sheet.get_all_values()[1:]  # пропустить заголовок

# Обработка каждой строки таблицы
for row_idx, (region, month_year, _) in enumerate(data, start=2):
    if region not in regions:
        print(f"⏭️ Пропуск неизвестного региона: {region}")
        continue

    month, year = parse_month_year(month_year)
    if is_after_may_2025(month, year):
        print(f"⏭️ Пропуск {region} {month_year} — после мая 2025")
        continue

    region_fc = ee.FeatureCollection(REGIONS_ASSET).filter(ee.Filter.eq('title', region))
    geometry = region_fc.geometry()
    start_date = f"{year}-{month:02d}-01"
    end_date = ee.Date(start_date).advance(1, 'month')

    collection = ee.ImageCollection('COPERNICUS/S2') \
        .filterDate(start_date, end_date) \
        .filterBounds(geometry) \
        .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', 80))

    if collection.size().getInfo() == 0:
        print(f"📭 Нет снимков: {region} {month_year}")
        sheet.update_cell(row_idx, 3, 'Нет снимков')
        continue

    # Маска облаков и сглаживание
    def mask_and_smooth(img):
        scl = img.select('SCL')
        cloud_mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10)).And(scl.neq(11))
        masked = img.updateMask(cloud_mask)
        return masked.resample('bicubic').convolve(ee.Kernel.gaussian(radius=1, sigma=1))

    image = collection.map(mask_and_smooth).median()
    image = image.visualize(bands=['TCI_R', 'TCI_G', 'TCI_B'], max=3000)

    # Генерация XYZ-ссылки
    map_id = ee.data.getMapId({'image': image})
    url = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{map_id['mapid']}/tiles/{{z}}/{{x}}/{{y}}?token={map_id['token']}"

    # QLR-экспорт и загрузка
    safe_region = region.replace(" ", "_").replace("'", "")
    filename = f"{safe_region}_{month_year.replace(' ', '_')}.qlr"
    qlr_path = generate_qlr_file(url, filename)
    download_url = upload_to_drive(qlr_path, filename, DRIVE_FOLDER_ID, credentials)

    # Обновление таблицы
    sheet.update_cell(row_idx, 3, download_url)
    print(f"✅ {region} {month_year} — {download_url}")
