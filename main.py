import ee
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime

# Авторизация Earth Engine
ee.Initialize()

# Авторизация Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)

# Открытие таблицы
sheet = client.open_by_key("1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY").worksheet("Sentinel-2 Покрытие")

# Загрузка ассета с регионами
regions_fc = ee.FeatureCollection("projects/ee-romantik1994/assets/region")

# Обработка каждой строки таблицы
rows = sheet.get_all_values()[1:]  # пропустить заголовок

for i, row in enumerate(rows, start=2):  # начиная со 2-й строки
    region_name = row[0].strip()
    month_year = row[1].strip()
    
    if len(row) > 2 and row[2].strip():  # пропустить, если URL уже заполнен
        continue

    try:
        # Парсинг даты
        date_obj = datetime.datetime.strptime(month_year, "%B %Y")
        start_date = date_obj.strftime("%Y-%m-01")
        end_date = (date_obj + datetime.timedelta(days=32)).replace(day=1).strftime("%Y-%m-01")

        # Фильтрация региона
        region = regions_fc.filter(ee.Filter.eq('title', region_name)).geometry()

        # Фильтрация по времени и облакам
        collection = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterBounds(region)
            .filterDate(start_date, end_date)
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 50))
            .map(lambda img: img.updateMask(img.select("SCL").neq(3))  # remove cloud shadows
                                  .updateMask(img.select("SCL").neq(8)))  # remove clouds
        )

        # Получение медианы и TCI визуализация
        mosaic = collection.median().clip(region).resample('bicubic').convolve(ee.Kernel.gaussian(2, 1))

        vis_params = {
            "bands": ["TCI_R", "TCI_G", "TCI_B"],
            "min": 0,
            "max": 3000
        }

        # Генерация карты и сборка корректной ссылки
        map_info = ee.data.getMapId({"image": mosaic.visualize(**vis_params)})
        mapid = map_info["mapid"]
        xyz_url = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{mapid}/tiles/{{z}}/{{x}}/{{y}}"

        # Запись в таблицу
        sheet.update_cell(i, 3, xyz_url)
        print(f"[{region_name} — {month_year}] ✅")

    except Exception as e:
        print(f"[{region_name} — {month_year}] ❌ Ошибка: {e}")
