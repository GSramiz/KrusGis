import ee
import json
import os

# Авторизация
service_account_info = json.loads(os.environ["GEE_CREDENTIALS"])
credentials = ee.ServiceAccountCredentials(
    service_account_info["client_email"],
    key_data=json.dumps(service_account_info)
)
ee.Initialize(credentials)

# Загружаем изображение из ассета
image = ee.Image("projects/ee-romantik1994/assets/mosaic_Belgorodskaya_2022-05")

tile_info = ee.data.getMapId({
    "image": image,
    "bands": ["B4", "B3", "B2"],
    "min": "0,0,0",         # ✅ строка, а не список
    "max": "3000,3000,3000" # ✅ строка
})

xyz_url = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{tile_info['mapid']}/tiles/{{z}}/{{x}}/{{y}}"
print("XYZ ссылка:", xyz_url)
