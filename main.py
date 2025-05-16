import ee
import json
import os
import traceback

# Логирование
def log_error(context, error):
    print(f"\n❌ ОШИБКА в {context}: {type(error).__name__}: {error}")
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

# Геометрия региона
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

# Основная функция
def test_mosaic_region():
    try:
        region_name = "Алтайский край"
        start = "2022-05-01"
        end = "2022-06-01"

        print(f"\n🗺️ Регион: {region_name}, период: {start} → {end}")
        geometry = get_geometry_from_asset(region_name)
        region_area = geometry.area().divide(1e6)  # км²

        # Базовая коллекция
        raw = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterDate(start, end)
            .filterBounds(geometry)
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 40))
            .map(mask_clouds)
            .map(lambda img: img.resample("bicubic").select(["TCI_R", "TCI_G", "TCI_B"]))
            .sort("CLOUDY_PIXEL_PERCENTAGE")
        )

        count_total = raw.size().getInfo()
        if count_total == 0:
            print("❌ Нет снимков за указанный период")
            return

        print(f"📸 Доступно снимков: {count_total}")

        # Итерация и сбор покрытия
        def accumulate_coverage(img_list, threshold_km2):
            covered = ee.Image(0).rename("coverage").updateMask(ee.Image(0))
            used = []
            total_covered = ee.Number(0)

            for i in range(img_list.size().getInfo()):
                img = ee.Image(img_list.get(i))
                new_mask = img.mask().reduce(ee.Reducer.anyNonZero()).rename("mask")
                incremental = new_mask.And(covered.Not())

                incremental_area = incremental.multiply(ee.Image.pixelArea()).reduceRegion(
                    reducer=ee.Reducer.sum(),
                    geometry=geometry,
                    scale=20,
                    maxPixels=1e9
                ).get("mask")

                inc_area_km2 = ee.Number(incremental_area).divide(1e6)

                if inc_area_km2.getInfo() == 0:
                    continue

                total_covered = total_covered.add(inc_area_km2)
                covered = covered.Or(new_mask)
                used.append(img)

                if total_covered.gte(threshold_km2).getInfo():
                    break

            return ee.ImageCollection(used), total_covered

        threshold_km2 = region_area.multiply(0.9)
        img_list = raw.toList(raw.size())
        best_imgs, total_area = accumulate_coverage(img_list, threshold_km2)

        print(f"✅ Используем снимков: {best_imgs.size().getInfo()}")
        print(f"📐 Покрытие: {round(total_area.getInfo(), 2)} км² из {round(region_area.getInfo(), 2)} км²")

        # Мозаика
        mosaic = best_imgs.mosaic().clip(geometry)
        vis = {"bands": ["TCI_R", "TCI_G", "TCI_B"], "min": 0, "max": 255}
        tile_info = ee.data.getMapId({"image": mosaic, "visParams": vis})
        mapid = tile_info["mapid"].split("/")[-1]
        xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{mapid}/tiles/{{z}}/{{x}}/{{y}}"

        print("\n🛰️ XYZ-ссылка:")
        print(xyz)

    except Exception as e:
        log_error("test_mosaic_region", e)

# Запуск
if __name__ == "__main__":
    try:
        initialize_services()
        test_mosaic_region()
    except Exception as e:
        log_error("main", e)
        exit(1)
