import ee
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from pprint import pprint
import json

def update_gee_links():
    # 1. Инициализация GEE ee.Initialize()
 print("GEE успешно инициализирован")

    # 2. Настройки
    CONFIG = {
        "spreadsheet_id": "1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY",
        "sheet_name": "1945273513",  # ID вкладки
        "geometry": ee.Geometry.Rectangle([30, 50, 180, 80]),  # Границы РФ
        "bands": ["B4", "B3", "B2"],  # RGB-каналы
        "min": 0,
        "max": 3000,
        "max_clouds": 30  # Максимальная облачность (30%)
    }

    # 3. Авторизация Google Sheets
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(G_SHEETS_KEY), scope
    )
    client = gspread.authorize(creds)
    print("✅ Авторизация Google Sheets успешна")

    # 4. Поиск лучшего снимка (минимальная облачность)
    def find_best_image(start_date, end_date):
        collection = ee.ImageCollection("COPERNICUS/S2_SR") \
            .filterDate(start_date, end_date) \
            .filterBounds(CONFIG["geometry"]) \
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", CONFIG["max_clouds"])) \
            .sort("CLOUDY_PIXEL_PERCENTAGE")  # Сортируем по облачности

        # Берём снимок с минимальной облачностью
        best_image = ee.Image(collection.first())

        cloud_percent = best_image.get("CLOUDY_PIXEL_PERCENTAGE").getInfo()
        if cloud_percent is None:
            return None, None

        print(f"Найден снимок с облачностью {cloud_percent}%")
        return best_image, cloud_percent

    # 5. Генерация URL
    def generate_url(image):
        return image.getThumbURL({
            "bands": CONFIG["bands"],
            "min": CONFIG["min"],
            "max": CONFIG["max"],
            "region": CONFIG["geometry"]
        })

    # 6. Обновление таблицы
    def update_sheet():
        sheet = client.open_by_key(CONFIG["spreadsheet_id"]).worksheet(CONFIG["sheet_name"])
        data = sheet.get_all_values()

        for row_idx, row in enumerate(data[1:], start=2):  # Пропускаем заголовок
            region, month_year, current_url = row[0], row[1], row[2]

            if region != "Вся РФ" or not month_year:
                continue

            try:
                month, year = month_year.split()
                start_date = f"{year}-{month}-01"
                end_date = ee.Date(start_date).advance(1, "month").format("YYYY-MM-dd").getInfo()

                print(f"\n🔍 Обработка: {month_year}...")

                best_image, cloud_percent = find_best_image(start_date, end_date)

                if best_image:
                    url = generate_url(best_image)
                    if url:
                        sheet.update_cell(row_idx, 3, url)  # Обновляем колонку C
                        print(f"✅ Обновлено: {month_year} (облачность {cloud_percent}%)")
                    else:
                        sheet.update_cell(row_idx, 3, "Ошибка генерации URL")
                else:
                    sheet.update_cell(row_idx, 3, "Нет снимков")
                    print(f"⚠️ Нет подходящих снимков для {month_year}")

            except Exception as e:
                print(f"❌ Ошибка обработки строки {row_idx}:", str(e))
                sheet.update_cell(row_idx, 3, f"Ошибка: {str(e)}")

    # Запуск обновления
    update_sheet()

if __name__ == "__main__":
    update_gee_links()
