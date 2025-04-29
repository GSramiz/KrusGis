import ee
import gspread
from datetime import datetime

def update_gee_links():
    # 1. Инициализация GEE
    try:
        ee.Initialize()
        print("GEE успешно инициализирован")
    except Exception as e:
        print("Ошибка GEE:", str(e))
        return

    # 2. Ваш код генерации ссылок (пример для одной ссылки)
    try:
        collection = ee.ImageCollection('COPERNICUS/S2_SR') \
            .filterDate('2024-05-01', '2024-05-31') \
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20))
        
        image = collection.median()
        url = image.getThumbURL({'bands': ['B4', 'B3', 'B2'], 'min': 0, 'max': 3000})
        
        print("Сгенерирована ссылка:", url)
        return url
    except Exception as e:
        print("Ошибка генерации ссылки:", str(e))
        return None

if __name__ == "__main__":
    update_gee_links()