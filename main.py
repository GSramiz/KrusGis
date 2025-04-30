import ee
import gspread
import json
import os
from oauth2client.service_account import ServiceAccountCredentials

def initialize_services():
    """Инициализация сервисов с улучшенным логированием"""
    try:
        # 1. Загрузка credentials из переменных окружения
        service_account_info = json.loads(os.environ["GEE_CREDENTIALS"])
        
        # 2. Инициализация Earth Engine
        credentials = ee.ServiceAccountCredentials(
            service_account_info['client_email'],
            key_data=json.dumps(service_account_info)
        )
        ee.Initialize(credentials)
        print("✅ Earth Engine инициализирован")

        # 3. Инициализация Google Sheets
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        sheets_client = gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
        )
        print("✅ Google Sheets авторизован")
        
        return sheets_client

    except Exception as e:
        print(f"❌ Ошибка инициализации: {str(e)}")
        raise  # Пробрасываем исключение для видимости в CI/CD

def update_sheet(sheets_client):
    """Обновление таблицы с детальным логированием"""
    try:
        config = {
            "spreadsheet_id": "1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY",
            "sheet_name": "1945273513",
            "geometry": ee.Geometry.Rectangle([30, 50, 180, 80])
        }

        print("⌛ Открываю таблицу...")
        sheet = sheets_client.open_by_key(config["spreadsheet_id"]).worksheet(config["sheet_name"])
        
        # Тестовое обновление ячейки
        sheet.update_cell(1, 1, "Тест из GitHub Actions")
        print("✅ Тестовое обновление выполнено")

    except Exception as e:
        print(f"❌ Ошибка при работе с таблицей: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        print("🚀 Запуск скрипта")
        client = initialize_services()
        update_sheet(client)
        print("🎉 Скрипт успешно выполнен")
    except Exception as e:
        print(f"💥 Критическая ошибка: {str(e)}")
        exit(1)
