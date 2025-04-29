import ee
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from pprint import pprint

# 1. Настройки
CONFIG = {
    "spreadsheet_id": "1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY",
    "sheet_name": "1945273513",  # ID вкладки
    "geometry": ee.Geometry.Rectangle([30, 50, 180, 80]),  # Границы РФ
    "bands": ["B4", "B3", "B2"],  # RGB-каналы
    "min": 0,
    "max": 3000,
    "max_clouds": 30  # Максимальная облачность (30%)
}

# 2. Авторизация
def auth():
    try:
        ee.Initialize()
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            "service-account.json", scope
        )
        client = gspread.authorize(creds)
        print("✅ Авторизация успешна")
        return client
    except Exception as e:
        print("❌ Ошибка авторизации:", str(e))
        return None

# 3. Поиск лучшего снимка (минимальная облачность)
def find_best_image(start_date, end_date):
    try:
        collection = ee.ImageCollection("COPERNICUS/S2_SR") \
            .filterDate(start_date, end_date) \
            .filterBounds(CONFIG["geometry"]) \
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", CONFIG["max_clouds"])) \
            .sort("CLOUDY_PIXEL_PERCENTAGE")  # Сортируем по облачности
        
        # Берём снимок с минимальной облачностью
        best_image = ee.Image(collection.first())
        
        # Проверяем, что снимок существует
        cloud_percent = best_image.get("CLOUDY_PIXEL_PERCENTAGE").getInfo()
        if cloud_percent is None:
            return None, None
        
        return best_image, cloud_percent
    except Exception as e:
        print(f"❌ Ошибка поиска снимка ({start_date} - {end_date}):", str(e))
        return None, None

# 4. Генерация URL
def generate_url(image):
    try:
        return image.getThumbURL({
            "bands": CONFIG["bands"],
            "min": CONFIG["min"],
            "max": CONFIG["max"],
            "region": CONFIG["geometry"]
        })
    except Exception as e:
        print("❌ Ошибка генерации URL:", str(e))
        return None

# 5. Обновление таблицы
def update_sheet(client):
    try:
        sheet = client.open_by_key(CONFIG["spreadsheet_id"]).worksheet_by_id(int(CONFIG["sheet_name"]))
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
                
    except Exception as e:
        print("❌ Критическая ошибка:", str(e))

if __name__ == "__main__":
    client = auth()
    if client:
        update_sheet(client)
