import ee
import json
import os
import traceback

# Логирование ошибок
def log_error(context, error):
    print(f"\n❌ ОШИБКА в {context}:")
    print(f"Тип: {type(error).__name__}")
    print(f"Сообщение: {str(error)}")
    traceback.print_exc()
    print("=" * 50)

# Инициализация Earth Engine
def initialize_services():
    try:
        print("\n🔧 Инициализация Earth Engine...")

        service_account_info = json.loads(os.environ["GEE_CREDENTIALS"])

        credentials = ee.ServiceAccountCredentials(
            service_account_info["client_email"],
            key_data=json.dumps(service_account_info)
        )
        ee.Initialize(credentials)
        print("✅ EE инициализирован")

    except Exception as e:
        log_error("initialize_services", e)
        raise

# Получение геометрии региона
def get_geometry_from_asset(region_name):
    fc = ee.FeatureCollection("projects/ee-romantik1994/assets/region")
    region = fc.filter(ee.Filter.eq("title", region_name)).first()
    if region is None:
        raise ValueError(f"Регион '{region_name}' не найден в ассете")
    return region.geometry()

# Маскирование облаков по SCL
def mask_clouds(img):
    scl = img.select("SCL")
    cloud_mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    return img.updateMask(cloud_mask)

# Добавляем "оценку" облачности
def add_score(img):
    score = ee.Number(img.get("CLOUDY_PIXEL_PERCENTAGE")).multiply(-1)
    return img.addBands(ee.Image.constant(score).rename("score"))

# Тестируем Алтайский край, май 2022
def test_single_region():
    try:
        region_name = "Алтайский край"
        start = "2022-05-01"
        end = "2022-06-01"

        print(f"\n🗺️ Регион: {region_name}, период: {start} → {end}")

        geometry = get_geometry_from_asset(region_name)

        collection = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterDate(start, end)
            .filterBounds(geometry)
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 40))
            .map(mask_clouds)
            .map(add_score)
            .map(lambda img: img.select(["TCI_R", "TCI_G", "TCI_B", "score"]).resample("bicubic"))
            .sort("score")
        )

        count = collection.size().getInfo()
        if count == 0:
            print("❌ Нет снимков за указанный период")
            return

        best_image = ee.Image(collection.first()).clip(geometry)

        vis = {"bands": ["TCI_R", "TCI_G", "TCI_B"], "min": 0, "max": 255}
        tile_info = ee.data.getMapId({
            "image": best_image,
            "visParams": vis
        })
        mapid = tile_info["mapid"].split("/")[-1]
        xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{mapid}/tiles/{{z}}/{{x}}/{{y}}"

        print("\n✅ УСПЕХ! XYZ-ссылка:")
        print(xyz)

    except Exception as e:
        log_error("test_single_region", e)

# Точка входа
if __name__ == "__main__":
    try:
        initialize_services()
        test_single_region()
    except Exception as e:
        log_error("main", e)
        exit(1)
