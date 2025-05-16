import ee
import json
import os
import traceback

def log_error(context, error):
    print(f"\n❌ ОШИБКА в {context}:")
    print(f"Тип: {type(error).__name__}")
    print(f"Сообщение: {str(error)}")
    traceback.print_exc()
    print("=" * 50)

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

def get_geometry_from_asset(region_name):
    fc = ee.FeatureCollection("projects/ee-romantik1994/assets/region")
    region = fc.filter(ee.Filter.eq("title", region_name)).first()
    if region is None:
        raise ValueError(f"Регион '{region_name}' не найден в ассете")
    return region.geometry()

def mask_clouds(img):
    scl = img.select("SCL")
    cloud_mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    return img.updateMask(cloud_mask)

def calculate_coverage(masked_img, region):
    mask = masked_img.mask().reduce(ee.Reducer.min())
    area_image = ee.Image.pixelArea().updateMask(mask)
    covered_area = area_image.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=region,
        scale=20,
        maxPixels=1e10
    ).getNumber("area")

    total_area = region.area(1)
    return covered_area.divide(total_area)

def build_mosaic_with_iterative_coverage(collection, region, min_coverage=0.95):
    sorted_imgs = collection.sort("CLOUDY_PIXEL_PERCENTAGE").toList(collection.size())
    region_area = region.area(1)

    selected = []
    cumulative = ee.Image(0).selfMask()  # пустая начальная маска

    i = 0
    while i < sorted_imgs.size().getInfo():
        img = ee.Image(sorted_imgs.get(i))
        img_masked = mask_clouds(img).resample("bicubic").select(["TCI_R", "TCI_G", "TCI_B"])
        selected.append(img_masked)

        mosaic = ee.ImageCollection(selected).mosaic().clip(region)

        coverage = calculate_coverage(mosaic, region).getInfo()
        print(f"🧩 [{i+1}] Покрытие: {round(coverage*100, 2)}%")

        if coverage >= min_coverage:
            break
        i += 1

    print(f"📸 Выбрано снимков: {len(selected)}")

    final_mosaic = ee.ImageCollection(selected).mosaic().clip(region)
    return final_mosaic

def test_mosaic_region():
    try:
        region_name = "Алтайский край"
        start = "2022-05-01"
        end = "2022-06-01"

        print(f"\n🗺️ Регион: {region_name}, период: {start} → {end}")
        geometry = get_geometry_from_asset(region_name)

        raw_collection = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterDate(start, end)
            .filterBounds(geometry)
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 40))
        )

        print(f"📥 Доступно снимков: {raw_collection.size().getInfo()}")

        mosaic = build_mosaic_with_iterative_coverage(raw_collection, geometry, min_coverage=0.95)

        vis = {"bands": ["TCI_R", "TCI_G", "TCI_B"], "min": 0, "max": 255}
        tile_info = ee.data.getMapId({
            "image": mosaic,
            "visParams": vis
        })
        mapid = tile_info["mapid"].split("/")[-1]
        xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{mapid}/tiles/{{z}}/{{x}}/{{y}}"

        print("\n✅ Мозаика построена. XYZ-ссылка:")
        print(xyz)

    except Exception as e:
        log_error("test_mosaic_region", e)

if __name__ == "__main__":
    try:
        initialize_services()
        test_mosaic_region()
    except Exception as e:
        log_error("main", e)
        exit(1)
