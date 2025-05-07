import ee
import gspread
import json
import os
import traceback
from oauth2client.service_account import ServiceAccountCredentials

# Логирование ошибок
def log_error(context, error):
    print(f"\n❌ ОШИБКА в {context}:")
    print(f"Тип: {type(error).__name__}")
    print(f"Сообщение: {str(error)}")
    traceback.print_exc()
    print("=" * 50)

# Инициализация Earth Engine и Google Sheets
def initialize_services():
    try:
        print("\n🔧 Инициализация сервисов...")

        service_account_info = json.loads(os.environ["GEE_CREDENTIALS"])

        credentials = ee.ServiceAccountCredentials(
            service_account_info["client_email"],
            key_data=json.dumps(service_account_info)
        )
        ee.Initialize(credentials)
        print("✅ Earth Engine: инициализирован")

        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        sheets_client = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
        )
        print("✅ Google Sheets: авторизация прошла успешно")
        return sheets_client

    except Exception as e:
        log_error("initialize_services", e)
        raise

# Перевод месяца из строки в номер
def month_str_to_number(name):
    months = {
        "Январь": "01", "Февраль": "02", "Март": "03", "Апрель": "04",
        "Май": "05", "Июнь": "06", "Июль": "07", "Август": "08",
        "Сентябрь": "09", "Октябрь": "10", "Ноябрь": "11", "Декабрь": "12"
    }
    return months.get(name.strip().capitalize(), None)

# Загрузка регионов
fc = ee.FeatureCollection("projects/ee-romantik1994/assets/region")

def get_geometry(region_name):
    feature = fc.filter(ee.Filter.eq("title", region_name)).first()
    return feature.geometry() if feature else None

def get_month_date_range(month_year_str):
    dt = datetime.strptime(month_year_str, "%B %Y")
    start = dt.replace(day=1)
    end = dt.replace(day=calendar.monthrange(dt.year, dt.month)[1])
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), dt

def generate_cloud_free_mosaic(geometry, start_date, end_date):
    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterDate(start_date, end_date)
        .filterBounds(geometry)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 40))
        .limit(10)
    )

    def mask_clouds(img):
        scl = img.select("SCL")
        mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10)).And(scl.neq(11))
        return img.updateMask(mask)

    kernel = ee.Kernel.gaussian(radius=2, sigma=1)
    mosaic = (
        collection.map(mask_clouds)
        .mosaic()
        .resample("bicubic")
        .select(["TCI_R", "TCI_G", "TCI_B"])
        .convolve(kernel)
    )
    return mosaic

def generate_xyz_url(image, vis_params):
    map_id = ee.Image(image).getMapId(vis_params)
    return f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{map_id['mapid']}/tiles/{{z}}/{{x}}/{{y}}"

# Текущая дата запуска скрипта
today = datetime.today()

# Основной цикл по строкам
rows = sheet.get_all_values()[1:]  # Пропустить заголовок
for i, row in enumerate(rows, start=2):  # Учёт строки заголовка
    try:
        region_name, month_year, _ = row[0], row[1], row[2]
        geom = get_geometry(region_name)
        if not geom:
            print(f"❌ Регион '{region_name}' не найден")
            continue

        start, end, target_date = get_month_date_range(month_year)

        # Пропуск будущих месяцев
        if target_date.year > today.year or (target_date.year == today.year and target_date.month > today.month):
            sheet.update_cell(i, 3, "Нет снимков")
            print(f"ℹ️ {region_name} — {month_year} — будущий месяц, пропуск")
            continue

        image = generate_cloud_free_mosaic(geom, start, end)
        xyz_url = generate_xyz_url(image, {"bands": ["TCI_R", "TCI_G", "TCI_B"], "min": 0, "max": 3000})
        sheet.update_cell(i, 3, xyz_url)
        print(f"✅ {region_name} — {month_year} — ссылка обновлена")
    except Exception as e:
        print(f"❌ Ошибка в строке {i}: {e}")
