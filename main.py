import ee
import datetime
import calendar
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Авторизация в Earth Engine
ee.Initialize()

# Авторизация в Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)

# Открытие таблицы
spreadsheet = client.open_by_key("1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY")
worksheet = spreadsheet.worksheet("Sentinel-2 Покрытие")

# Получение уникальных регионов и дат
data = worksheet.get_all_values()[1:]  # Пропускаем заголовок
regions_and_dates = [(row[0], row[1]) for row in data if row[2] == ""]

# Ассет с регионами
regions_fc = ee.FeatureCollection("projects/ee-romantik1994/assets/region")

def generate_url(region_name, year, month):
    # Получаем регион по названию
    region = regions_fc.filter(ee.Filter.eq("title", region_name)).first()
    geometry = region.geometry()

    start_date = datetime.date(year, month, 1)
    end_date = datetime.date(year, month, calendar.monthrange(year, month)[1])

    collection = ee.ImageCollection("COPERNICUS/S2_HARMONIZED") \
        .filterBounds(geometry) \
        .filterDate(str(start_date), str(end_date)) \
        .filter(ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", 80))

    def add_quality(image):
        scl = image.select("SCL")
        mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
        return image.updateMask(mask).set("CLOUDY_PIXEL_PERCENTAGE", image.get("CLOUDY_PIXEL_PERCENTAGE"))

    filtered = collection.map(add_quality)

    best = filtered.sort("CLOUDY_PIXEL_PERCENTAGE").mosaic().clip(geometry)
    tci = best.select(["TCI_R", "TCI_G", "TCI_B"]).resample("bicubic")

    kernel = ee.Kernel.gaussian(radius=1, sigma=1, units="pixels")
    smoothed = tci.convolve(kernel)

    vis = {"bands": ["TCI_R", "TCI_G", "TCI_B"], "min": 0, "max": 255}
    vis_image = smoothed.visualize(**vis)

    map_info = ee.data.getMapId({"image": vis_image})
    xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{map_info['mapid']}/tiles/{{z}}/{{x}}/{{y}}?token={map_info['token']}"
    return xyz

# Обновляем таблицу
for i, (region_name, month_year) in enumerate(regions_and_dates):
    try:
        print(f"Обработка: {region_name} — {month_year}")
        month_name, year = month_year.split()
        month = list(calendar.month_name).index(month_name)
        year = int(year)

        url = generate_url(region_name, year, month)
        worksheet.update_cell(i + 2, 3, url)

    except Exception as e:
        print(f"Ошибка при обработке {region_name} — {month_year}: {e}")
