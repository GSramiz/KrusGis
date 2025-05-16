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

def estimate_clear_coverage(img, region):
    footprint = img.geometry().intersection(region, 1)
    intersection_area = footprint.area(1)
    cloudy = ee.Number(img.get('CLOUDY_PIXEL_PERCENTAGE'))
    clear_area = intersection_area.multiply(ee.Number(1).subtract(cloudy.divide(100)))
    return clear_area

def build_mosaic_with_incremental_check(collection, region, min_coverage=0.95):
    sorted_imgs = collection.sort("CLOUDY_PIXEL_PERCENTAGE")
    region_area = region.area(1)

    selected_imgs = []
    total_clear_area = ee.Number(0)

    imgs = sorted_imgs.toList(sorted_imgs.size())
    count = imgs.size().getInfo()

    for i in range(count):
        img = ee.Image(imgs.get(i))
        clear_area = estimate_clear_coverage(img, region)
        total_clear_area = total_clear_area.add(clear_area)
        selected_imgs.append(img)

        if total_clear_area.divide(region_area).getInfo() >= min_coverage:
            break

    print(f"📸 Выбрано снимков по метаданным: {len(selected_imgs)}")

    # Строим начальную мозаику
    ic = ee.ImageCollection.fromImages(selected_imgs).map(mask_clouds)
    mosaic = ic.mosaic().clip(region)

    # Оцениваем фактическое покрытие
    actual_coverage = mosaic.mask().reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=region,
        scale=20,
        maxPixels=1e10
    ).getInfo()

    coverage_value = list(actual_coverage.values())[0] if actual_coverage else 0
    print(f"📐 Фактическое покрытие: {round(coverage_value * 100, 2)}%")

    # Если не хватает — добираем по одному
    idx = len(selected_imgs)
    while coverage_value < min_coverage and idx < count:
        next_img = ee.Image(imgs.get(idx))
        selected_imgs.append(next_img)
        ic = ee.ImageCollection.fromImages(selected_imgs).map(mask_clouds)
        mosaic = ic.mosaic().clip(region)

        actual_coverage = mosaic.mask().reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=region,
            scale=20,
            maxPixels=1e10
        ).getInfo()

        coverage_value = list(actual_coverage.values())[0] if actual_coverage else 0
        print(f"➕ Добавлен снимок {idx+1}, новое покрытие: {round(coverage_value * 100, 2)}%")
        idx += 1

    return mosaic

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

        mosaic = build_mosaic_with_incremental_check(raw_collection, geometry, min_coverage=0.95)

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
