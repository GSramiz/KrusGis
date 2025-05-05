# Импорты
import ee
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Авторизация в Earth Engine и Google Sheets
ee.Initialize()
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)

#  Импорт Google Sheets
spreadsheet = client.open_by_key('1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY')
sheet = spreadsheet.worksheet('Sentinel-2 Покрытие')
data = sheet.get("A2:C821")

# 📅 Месяцы
month_map = {
    'январь': '01', 'февраль': '02', 'март': '03', 'апрель': '04',
    'май': '05', 'июнь': '06', 'июль': '07', 'август': '08',
    'сентябрь': '09', 'октябрь': '10', 'ноябрь': '11', 'декабрь': '12'
}

# Коллекция регионов
regions = ee.FeatureCollection("projects/ee-romantik1994/assets/region")

# Обработка строк
for i, row in enumerate(data):
    region_name = row[0]
    month_year = row[1]

    if not region_name or not month_year:
        continue

    parts = month_year.lower().split()
    if len(parts) != 2:
        continue
    month = month_map.get(parts[0])
    year = parts[1]
    if not month or not year:
        continue

    start = ee.Date(f"{year}-{month}-01")
    end = start.advance(1, 'month')

    region = regions.filter(ee.Filter.eq('title', region_name)).geometry()

    vis_params = {
        'bands': ['TCI_R', 'TCI_G', 'TCI_B'],
        'min': 0,
        'max': 255
    }

    collection = (ee.ImageCollection("COPERNICUS/S2_SR")
                  .filterDate(start, end)
                  .filterBounds(region)
                  .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 60))
                  .map(lambda img: img.select(['TCI_R', 'TCI_G', 'TCI_B'])
                                .resample('bicubic')
                                .copyProperties(img, img.propertyNames())))

    mosaic = collection.mosaic().clip(region)

    kernel = ee.Kernel.gaussian(radius=1.2, sigma=1.2, units='pixels', normalize=True)
    smoothed = mosaic.convolve(kernel)

    try:
        mapid_dict = ee.Image(smoothed.visualize(**vis_params)).getMapId()
        mapid = mapid_dict['mapid']
        token = mapid_dict['token']
        xyz_url = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{mapid}/tiles/{{z}}/{{x}}/{{y}}"
    except Exception as e:
        xyz_url = f"Ошибка: {str(e)}"

    sheet.update_cell(i + 2, 3, xyz_url)
