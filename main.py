import ee 
import gspread
import json
import os
import traceback
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

#  Логирование ошибок
def log_error(context, error):
    print(f"\n❌ ОШИБКА в {context}:")
    print(f"Тип: {type(error).__name__}")
    print(f"Сообщение: {str(error)}")
    traceback.print_exc()
    print("=" * 50)

#  Инициализация Earth Engine и Google Sheets
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
        drive_service = build("drive", "v3", credentials=credentials)
        print("✅ Google Sheets и Drive: авторизация прошла успешно")
        return sheets_client, drive_service

    except Exception as e:
        log_error("initialize_services", e)
        raise

#  Перевод месяца
def month_str_to_number(name):
    months = {
        "Январь": "01", "Февраль": "02", "Март": "03", "Апрель": "04",
        "Май": "05", "Июнь": "06", "Июль": "07", "Август": "08",
        "Сентябрь": "09", "Октябрь": "10", "Ноябрь": "11", "Декабрь": "12"
    }
    return months.get(name.strip().capitalize(), None)

#  Геометрия региона
def get_geometry_from_asset(region_name):
    fc = ee.FeatureCollection("projects/ee-romantik1994/assets/region")
    region = fc.filter(ee.Filter.eq("title", region_name)).first()
    if region is None:
        raise ValueError(f"Регион '{region_name}' не найден в ассете")
    return region.geometry()

#  Создание .qlr-файла
def create_qlr_file(region, date_str, xyz_url):
    content = f'''<qgis styleCategories="AllStyleCategories" version="3.28">
  <layer-tree-layer id="{region}_{date_str}" name="{region} {date_str}" providerKey="wms" checked="Qt::Checked">
    <customproperties/>
  </layer-tree-layer>
  <maplayer type="raster" name="{region} {date_str}" layername="{region} {date_str}" srs="EPSG:3857" url="{xyz_url}" provider="wms">
    <wmsLayers><layer>Sentinel-2</layer></wmsLayers>
    <tileMatrixSet>GoogleMapsCompatible</tileMatrixSet>
    <wmsFormat>image/png</wmsFormat>
  </maplayer>
</qgis>'''
    filename = f"{region}_{date_str.replace(' ', '_')}.qlr"
    filepath = f"/tmp/{filename}"
    with open(filepath, "w") as f:
        f.write(content)
    return filepath, filename

#  Загрузка в Google Drive
def upload_to_drive(service_account_info, file_path, file_name):
    try:
        import json
        import googleapiclient.discovery
        from googleapiclient.http import MediaFileUpload
        from google.oauth2 import service_account

        folder_id = "1IAAEI0NDp_X5iy78jmGPzwJcF6POykRd"

        # Преобразуем строку в словарь, если нужно
        if isinstance(service_account_info, str):
            try:
                service_account_info = json.loads(service_account_info)
            except json.JSONDecodeError:
                raise ValueError("❌ service_account_info — строка, но невалидный JSON.")

        creds = service_account.Credentials.from_service_account_info(service_account_info)
        drive_service = googleapiclient.discovery.build("drive", "v3", credentials=creds)

        # Проверка доступа к папке
        try:
            _ = drive_service.files().get(fileId=folder_id, fields="id").execute()
        except Exception as e:
            raise PermissionError(
                f"❌ Нет доступа к папке с ID {folder_id}. Убедитесь, что сервисный аккаунт "
                f"{service_account_info.get('client_email', 'неизвестно')} добавлен в доступ к папке с ролью 'Редактор'."
            )

        file_metadata = {
            "name": file_name,
            "parents": [folder_id]
        }
        media = MediaFileUpload(file_path, mimetype="application/xml")
        file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()

        file_id = file.get("id")
        return f"https://drive.google.com/uc?id={file_id}&export=download"

    except PermissionError as pe:
        log_error("upload_to_drive (доступ к папке)", pe)
        return f"Ошибка доступа: {str(pe)}"
    except Exception as e:
        log_error("upload_to_drive", e)
        return f"Ошибка загрузки: {str(e)}"

#  Обновление таблицы
def update_sheet(sheets_client, drive_service):
    try:
        print("\n📊 Обновление таблицы")

        SPREADSHEET_ID = "1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY"
        SHEET_NAME = "Sentinel-2 Покрытие"

        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        data = worksheet.get_all_values()

        for row_idx, row in enumerate(data[1:], start=2):
            try:
                region, date_str = row[:2]
                if not region or not date_str:
                    continue

                parts = date_str.strip().split()
                if len(parts) != 2:
                    raise ValueError(f"Неверный формат даты: '{date_str}'")

                month_num = month_str_to_number(parts[0])
                year = parts[1]
                start = f"{year}-{month_num}-01"
                end = ee.Date(start).advance(1, "month")

                print(f"\n🌍 {region} — {start} - {end.format('YYYY-MM-dd').getInfo()}")
                geometry = get_geometry_from_asset(region)

                def mask_clouds(img):
                    scl = img.select("SCL")
                    cloud_classes = ee.List([3, 8, 9, 10])
                    mask = scl.remap(cloud_classes, ee.List.repeat(0, cloud_classes.length()), 1)
                    return img.updateMask(mask)

                collection = ee.ImageCollection("COPERNICUS/S2_SR") \
                    .filterDate(start, end) \
                    .filterBounds(geometry) \
                    .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 60)) \
                    .map(mask_clouds) \
                    .map(lambda img: img.select(["TCI_R", "TCI_G", "TCI_B"])
                         .resample("bicubic")
                         .copyProperties(img, img.propertyNames()))

                if collection.size().getInfo() == 0:
                    worksheet.update_cell(row_idx, 3, "Нет снимков")
                    print("⚠️ Нет снимков")
                    continue

                mosaic = collection.mosaic().clip(geometry)
                kernel = ee.Kernel.gaussian(1.2, 1.2, "pixels", True)
                smoothed = mosaic.convolve(kernel)

                vis_params = {"bands": ["TCI_R", "TCI_G", "TCI_B"], "min": 0, "max": 255}
                vis_image = smoothed.visualize(**vis_params)
                map_info = ee.data.getMapId({"image": vis_image})
                xyz_url = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{map_info['mapid']}/tiles/{{z}}/{{x}}/{{y}}?token={map_info['token']}"

                qlr_path, qlr_filename = create_qlr_file(region, date_str, xyz_url)
                download_url = upload_to_drive(qlr_path, qlr_filename, drive_service)
                worksheet.update_cell(row_idx, 3, download_url)
                print(f"✅ {region} {date_str} — загружено: {download_url}")

            except Exception as e:
                log_error(f"Строка {row_idx}", e)
                worksheet.update_cell(row_idx, 3, f"Ошибка: {str(e)[:100]}")

    except Exception as e:
        log_error("update_sheet", e)
        raise

#  Точка входа
if __name__ == "__main__":
    try:
        client, drive = initialize_services()
        update_sheet(client, drive)
        print("\n✅ Скрипт успешно завершен")
    except Exception as e:
        log_error("main", e)
        exit(1)
