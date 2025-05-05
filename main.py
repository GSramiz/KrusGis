import os
import json
import ee
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime
from dateutil.relativedelta import relativedelta

# ---------- Earth Engine AUTH ----------
SERVICE_ACCOUNT = 'gee-script@ee-romantik1994.iam.gserviceaccount.com'
KEY_PATH = 'service_account.json'

if not os.path.exists(KEY_PATH):
    key_content = os.environ.get("GEE_CREDENTIALS")
    if not key_content:
        raise Exception("GEE_CREDENTIALS environment variable not found.")
    with open(KEY_PATH, "w") as f:
        f.write(key_content)

credentials_ee = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEY_PATH)
ee.Initialize(credentials_ee)

# ---------- Google Sheets AUTH ----------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
credentials_gspread = Credentials.from_service_account_file(KEY_PATH, scopes=SCOPES)
gc = gspread.authorize(credentials_gspread)

# ---------- Константы ----------
TABLE_ID = "1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY"
SHEET_NAME = "Sentinel-2 Покрытие"
REGIONS_ASSET = "projects/ee-romantik1994/assets/region"

# ---------- Функции ----------

def create_qlr_file(region, date_str, xyz_url):
    safe_region = region.replace(" ", "_").replace("ё", "е").replace("Ё", "Е")
    filename = f"{safe_region}_{date_str}.qlr"
    filepath = os.path.join("/tmp", filename)
    qlr_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<qlr>
  <maplayer type="xyz" name="{region} {date_str}" hasScaleBasedVisibilityFlag="0" scaleBasedVisibilityMin="0" scaleBasedVisibilityMax="0">
    <id>{region}_{date_str}</id>
    <datasource>{xyz_url}</datasource>
    <layername>{region}_{date_str}</layername>
    <srs>EPSG:3857</srs>
    <layerOpacity>1</layerOpacity>
  </maplayer>
</qlr>"""
    with open(filepath, "w") as f:
        f.write(qlr_content)
    return filepath, filename

def upload_to_drive(service_account_info, filepath, filename):
    credentials = Credentials.from_service_account_info(service_account_info, scopes=["https://www.googleapis.com/auth/drive"])
    drive_service = build("drive", "v3", credentials=credentials)
    file_metadata = {"name": filename, "mimeType": "application/xml"}
    media_body = {"name": filename}
    with open(filepath, "rb") as f:
        media = f.read()
    uploaded_file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()
    return f"https://drive.google.com/uc?id={uploaded_file['id']}&export=download"

def log_error(stage, error):
    print(f"❌ Ошибка [{stage}]: {str(error)}")

def update_sheet():
    service_account_info = json.load(open(KEY_PATH))
    worksheet = gc.open_by_key(TABLE_ID).worksheet(SHEET_NAME)
    data = worksheet.get_all_values()[1:]  # без заголовка
    regions_fc = ee.FeatureCollection(REGIONS_ASSET)
    today = datetime.utcnow()

    for idx, row in enumerate(data, start=2):
        try:
            region = row[0].strip()
            date_str = row[1].strip()

            if len(row) > 2 and row[2].strip():
                continue  # Уже заполнено

            print(f"🗂️ Обработка: {region} — {date_str}")
            try:
                month_dt = datetime.strptime(date_str, "%B %Y")
            except Exception:
                worksheet.update_cell(idx, 3, "Неверный формат даты")
                continue

            if month_dt > today:
                worksheet.update_cell(idx, 3, "Будущий месяц")
                continue

            feature = regions_fc.filter(ee.Filter.eq("title", region)).first()
            if feature is None:
                worksheet.update_cell(idx, 3, "Регион не найден")
                continue

            geom = feature.geometry()
            start = ee.Date(f"{month_dt.year}-{month_dt.month:02d}-01")
            end = ee.Date(start.advance(1, "month"))

            collection = (
                ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                .filterBounds(geom)
                .filterDate(start, end)
                .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 80))
                .map(lambda img: img
                     .updateMask(img.select("SCL").neq(3))
                     .updateMask(img.select("SCL").neq(9))
                     .updateMask(img.select("SCL").neq(8))
                     .resample("bicubic"))
            )

            if collection.size().getInfo() == 0:
                worksheet.update_cell(idx, 3, "Нет снимков")
                continue

            best = collection.sort("CLOUDY_PIXEL_PERCENTAGE").first()
            vis = (
                best.visualize(bands=["TCI_R", "TCI_G", "TCI_B"], min=0, max=3000, forceRgbOutput=True)
                .convolve(ee.Kernel.gaussian(2, 1, "pixels"))
            )

            map_id = ee.Image(vis).getMapId()
            xyz_url = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{map_id['mapid']}/tiles/{{z}}/{{x}}/{{y}}"
            print("✅ XYZ:", xyz_url)

            qlr_path, qlr_name = create_qlr_file(region, date_str, xyz_url)
            drive_url = upload_to_drive(service_account_info, qlr_path, qlr_name)

            worksheet.update_cell(idx, 3, xyz_url)
            print(f"📤 Ссылка добавлена: {xyz_url}")

        except Exception as e:
            log_error(f"{region} — {date_str}", e)
            worksheet.update_cell(idx, 3, "Ошибка")

# ---------- Запуск ----------
if __name__ == "__main__":
    update_sheet()
