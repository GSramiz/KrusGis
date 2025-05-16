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

def get_clear_mask_area(image, region):
    masked = mask_clouds(image)
    mask = masked.select(0).mask()  # Получаем бинарную маску
    area_image = ee.Image.pixelArea().updateMask(mask)
    area = area_image.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=region,
        scale=20,
        maxPixels=1e10
    ).get("area")
    return ee.Number(area)

def build_mosaic_with_coverage(collection, region, min_coverage=0.95, batch_size=5):
    sorted_imgs = collection.sort("CLOUDY_PIXEL_PERCENTAGE")
    img_list = sorted_imgs.toList(sorted_imgs.size())
    total_imgs = img_list.size().getInfo()

    selected_imgs = []
    accumulated_area = 0
    region_area = region.area(1).getInfo()
    i = 0

    print(f"📦 Старт пакетной загрузки по {batch_size} снимков...")

    while i < total_imgs and (accumulated_area / region_area) < min_coverage:
        batch = img_list.slice(i, i + batch_size)
        batch = ee.List(batch)

        for j in range(batch.size().getInfo()):
            img = ee.Image(batch.get(j))
            clear_area = get_clear_mask_area(img, region).getInfo()
            if clear_area == 0:
                continue
            accumulated_area += clear_area
            selected_imgs.append(img)
            print(f"➕ Добавлен снимок ({i + j + 1}/{total_imgs}), накопленное покрытие: {accumulated_area / region_area:.2%}")
            if accumulated_area / region_area >= min_coverage:
                break
        i += batch_size

    print(f"📸 Всего выбрано снимков: {len(selected_imgs)}")

    ic = ee.ImageCollection.fromImages(selected_imgs)
    mosaic = ic.map(mask_clouds).map(lambda img: img.resample("bicubic").select(["TCI_R", "TCI_G", "TCI_B"])).mosaic().clip(region)
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

        mosaic = build_mosaic_with_coverage(raw_collection, geometry, min_coverage=0.95, batch_size=5)

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
