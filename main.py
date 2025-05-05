import ee
import datetime
import calendar
import urllib.parse
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Аутентификация Earth Engine
service_account = 'ee-romantik1994@ee-romantik1994.iam.gserviceaccount.com'
credentials = ee.ServiceAccountCredentials(service_account, 'GEE_CREDENTIALS.json')
ee.Initialize(credentials)

# Google Sheets setup
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
gs_credentials = ServiceAccountCredentials.from_json_keyfile_name('GEE_CREDENTIALS.json', scope)
gc = gspread.authorize(gs_credentials)
spreadsheet = gc.open_by_key('1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY')
worksheet = spreadsheet.worksheet('Sentinel-2 Покрытие')

# Доступ к регионам
regions = ee.FeatureCollection('projects/ee-romantik1994/assets/region')

# Получение всех значений из таблицы
table_data = worksheet.get_all_values()
header = table_data[0]
data_rows = table_data[1:]

# Перебор строк таблицы
for i, row in enumerate(data_rows, start=2):
    region_name, month_year, current_value = row[0], row[1], row[2] if len(row) > 2 else ''

    if current_value and current_value != 'Нет снимков':
        print(f'[{i}] Пропущено (уже заполнено): {region_name}, {month_year}')
        continue

    try:
        month_str, year_str = month_year.split()
        month = list(calendar.month_name).index(month_str)
        year = int(year_str)
    except:
        print(f'[{i}] Неверный формат даты: {month_year}')
        continue

    if (year > 2025) or (year == 2025 and month > 5):
        print(f'[{i}] Пропущено (будущее): {region_name}, {month_year}')
        continue

    region_feature = regions.filter(ee.Filter.eq('title', region_name)).first()
    if region_feature is None:
        print(f'[{i}] Регион не найден: {region_name}')
        continue

    geometry = region_feature.geometry()
    start_date = datetime.date(year, month, 1)
    end_day = calendar.monthrange(year, month)[1]
    end_date = datetime.date(year, month, end_day)

    collection = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
        .filterBounds(geometry)
        .filterDate(str(start_date), str(end_date))
        .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', 50))
        .map(lambda img: img.updateMask(img.select('SCL').neq(3)
                                              .And(img.select('SCL').neq(8))
                                              .And(img.select('SCL').neq(9))
                                              .And(img.select('SCL').neq(10)))
                          .resample('bicubic')
                          .copyProperties(img, img.propertyNames()))
    )

    mosaic = collection.qualityMosaic('CLOUDY_PIXEL_PERCENTAGE')
    vis = mosaic.visualize(**{'bands': ['TCI_R', 'TCI_G', 'TCI_B'], 'min': 0, 'max': 3000})

    try:
        mapid_dict = ee.data.getMapId({'image': vis})
        mapid = mapid_dict['mapid']
        # Ручная сборка ссылки без вложенных путей
        url = f'https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{mapid}/tiles/{{z}}/{{x}}/{{y}}'
        worksheet.update_cell(i, 3, url)
        print(f'[{i}] ✅ {region_name}, {month_year}')
    except Exception as e:
        print(f'[{i}] ❌ {region_name}, {month_year} — {str(e)}')
        worksheet.update_cell(i, 3, 'Нет снимков')
        time.sleep(1.0)
