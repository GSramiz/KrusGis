import ee
import json
import calendar
import time
import re
import gspread
from google.oauth2.service_account import Credentials

# ============ 1. Загрузка GEE credentials ============

with open("credentials.json", "r") as f:
    service_account_info = json.load(f)

# Авторизация в Earth Engine
credentials_ee = ee.ServiceAccountCredentials(
    email=service_account_info["client_email"],
    key_data=json.dumps(service_account_info)
)
ee.Initialize(credentials_ee)

# Авторизация в Google Sheets (через google-auth)
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
credentials_gsheets = Credentials.from_service_account_info(service_account_info, scopes=scope)
gc = gspread.authorize(credentials_gsheets)

# Таблица
SPREADSHEET_ID = "1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY"
worksheet = gc.open_by_key(SPREADSHEET_ID).sheet1

# Колонки: A — Регион, B — Месяц и год, C — URL покрытия (авто)
records = worksheet.get_all_values()[1:]  # без заголовка

# Загрузка регионов из Earth Engine
regions = ee.FeatureCollection("projects/ee-romantik1994/assets/region")

def generate_preview_url(region_title, month_year):
    """Генерация XYZ-ссылки для TCI мозаики с маской облаков и сглаживанием"""
    try:
        region_feature = regions.filter(ee.Filter.eq('title', region_title)).first()
        geom = region_feature.geometry()

        # Разбор даты
        match = re.match(r"([А-Яа-я]+)\s+(\d{4})", month_year)
        if not match:
            print(f"Неверный формат даты: {month_year}")
            return None

        month_name_rus, year = match.groups()
        month_map = {
            'Январь': 1, 'Февраль': 2, 'Март': 3, 'Апрель': 4, 'Май': 5,
            'Июнь': 6, 'Июль': 7, 'Август': 8, 'Сентябрь': 9, 'Октябрь': 10, 'Ноябрь': 11, 'Декабрь': 12
        }

        month = month_map.get(month_name_rus.capitalize())
        if not month:
            print(f"Не удалось распознать месяц: {month_name_rus}")
            return None

        start_date = f"{year}-{month:02d}-01"
        end_day = calendar.monthrange(int(year), month)[1]
        end_date = f"{year}-{month:02d}-{end_day}"

        # Получение коллекции S2
        col = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED') \
            .filterBounds(geom) \
            .filterDate(start_date, end_date) \
            .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', 80)) \
            .map(lambda img: img.updateMask(img.select('SCL').neq(3)
                                            .And(img.select('SCL').neq(8))
                                            .And(img.select('SCL').neq(9))
                                            .And(img.select('SCL').neq(10)))) \
            .select(['TCI_R', 'TCI_G', 'TCI_B']) \
            .map(lambda img: img.resample('bicubic'))

        # Мозаика и сглаживание
        mosaic = col.median().convolve(ee.Kernel.gaussian(radius=2, sigma=1, units='pixels')).clip(geom)

        map_id_dict = ee.Image(mosaic).getMapId({
            'min': 0, 'max': 3000,
            'bands': ['TCI_R', 'TCI_G', 'TCI_B'],
            'format': 'png'
        })

        xyz_url = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{map_id_dict['mapid']}/tiles/{{z}}/{{x}}/{{y}}?token={map_id_dict['token']}"
        return xyz_url

    except Exception as e:
        print(f"Ошибка для {region_title} {month_year}: {e}")
        return None


# ========== Обработка строк таблицы ==========

for i, row in enumerate(records, start=2):  # начиная со 2-й строки
    region, month_year, existing_url = row[:3]

    if existing_url.strip():
        continue  # уже заполнено

    print(f"Обработка: {region}, {month_year}")
    url = generate_preview_url(region, month_year)
    if url:
        worksheet.update_cell(i, 3, url)
        print(f"✅ Обновлено: {url}")
        time.sleep(1.5)  # чтобы не превышать лимиты
    else:
        print("❌ Не удалось сгенерировать URL")

print("🟢 Завершено.")
