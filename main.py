import ee
import os
import json
import calendar
import time
import re
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials

# ===== 1. Авторизация =====
service_account_raw = os.environ.get("GEE_CREDENTIALS")
if not service_account_raw:
    raise RuntimeError("Переменная среды GEE_CREDENTIALS не установлена")

try:
    service_account_info = json.loads(service_account_raw)
except json.JSONDecodeError:
    raise ValueError("GEE_CREDENTIALS содержит некорректный JSON")

credentials_ee = ee.ServiceAccountCredentials(
    email=service_account_info["client_email"],
    key_data=json.dumps(service_account_info)
)
ee.Initialize(credentials_ee)

scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
credentials_gsheets = Credentials.from_service_account_info(service_account_info, scopes=scope)
gc = gspread.authorize(credentials_gsheets)

# ===== 2. Работа с таблицей =====
SPREADSHEET_ID = "1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY"
worksheet = gc.open_by_key(SPREADSHEET_ID).sheet1
records = worksheet.get_all_values()[1:]  # без заголовка

# ===== 3. Коллекция регионов =====
regions = ee.FeatureCollection("projects/ee-romantik1994/assets/region")

# ===== 4. Генерация ссылки =====
def generate_preview_url(region_title, month_year):
    try:
        # Геометрия региона
        region_feature = regions.filter(ee.Filter.eq('title', region_title)).first()
        geom = region_feature.geometry()

        # Парсинг даты
        match = re.match(r"([А-Яа-я]+)\s+(\d{4})", month_year.strip())
        if not match:
            print(f"⚠️ Неверный формат даты: {month_year}")
            return "Нет снимков"

        month_name_rus, year = match.groups()
        month_map = {
            'Январь': 1, 'Февраль': 2, 'Март': 3, 'Апрель': 4, 'Май': 5,
            'Июнь': 6, 'Июль': 7, 'Август': 8, 'Сентябрь': 9,
            'Октябрь': 10, 'Ноябрь': 11, 'Декабрь': 12
        }

        month = month_map.get(month_name_rus.capitalize())
        if not month:
            print(f"⚠️ Не удалось распознать месяц: {month_name_rus}")
            return "Нет снимков"

        # Ограничение по времени
        year = int(year)
        if year > 2025 or (year == 2025 and month > 5):
            print(f"⏭ Пропуск: {month_year} превышает май 2025")
            return "Нет снимков"

        start_date = f"{year}-{month:02d}-01"
        end_day = calendar.monthrange(year, month)[1]
        end_date = f"{year}-{month:02d}-{end_day}"

        # Коллекция Sentinel-2
        col = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED') \
            .filterBounds(geom) \
            .filterDate(start_date, end_date) \
            .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', 80)) \
            .map(lambda img: img.updateMask(
                img.select('SCL').neq(3)
                .And(img.select('SCL').neq(8))
                .And(img.select('SCL').neq(9))
                .And(img.select('SCL').neq(10))
            )) \
            .select(['TCI_R', 'TCI_G', 'TCI_B']) \
            .map(lambda img: img.resample('bicubic'))

        size = col.size().getInfo()
        if size == 0:
            print(f"🕳 Нет снимков за {month_year} в регионе {region_title}")
            return "Нет снимков"

        # Мозаика + сглаживание
        mosaic = col.median().convolve(
            ee.Kernel.gaussian(radius=2, sigma=1, units='pixels')
        ).clip(geom)

        map_id_dict = ee.Image(mosaic).getMapId({
            'min': 0, 'max': 3000,
            'bands': ['TCI_R', 'TCI_G', 'TCI_B'],
            'format': 'png'
        })

        return f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{map_id_dict['mapid']}/tiles/{{z}}/{{x}}/{{y}}?token={map_id_dict['token']}"

    except Exception as e:
        print(f"❌ Ошибка ({region_title}, {month_year}): {e}")
        return "Нет снимков"

# ===== 5. Обработка =====
for i, row in enumerate(records, start=2):
    region, month_year, existing_url = row[:3]

    if existing_url.strip():
        continue

    print(f"🔄 Обработка: {region}, {month_year}")
    url_or_message = generate_preview_url(region.strip(), month_year.strip())

    worksheet.update_cell(i, 3, url_or_message)
    print(f"✅ Обновлено: {url_or_message}")
    time.sleep(1.5)

print("🟢 Завершено.")
