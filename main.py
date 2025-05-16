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

def get_visible_coverage(img, region):
    masked = mask_clouds(img)
    masked_area = masked.geometry().intersection(region, 1).area(1)
    region_area = region.area(1)
    return masked_area.divide(region_area)

def build_mosaic_with_coverage(collection, region, target_coverage=0.95):
    sorted_imgs = collection.sort("CLOUDY_PIXEL_PERCENTAGE")

    def accumulate(img, acc):
        acc = ee.List(acc)
        coverage_so_far = ee.Number(acc.get(0))
        imgs_so_far = ee.List(acc.get(1))

        coverage = get_visible_coverage(img, region)
        new_coverage = coverage_so_far.add(coverage)

        should_add = coverage_so_far.lt(target_coverage)

        updated_imgs = ee.Algorithms.If(should_add, imgs_so_far.add(img), imgs_so_far)
        updated_cov = ee.Algorithms.If(should_add, new_coverage, coverage_so_far)

        return ee.List([updated_cov, updated_imgs])

    init = ee.List([ee.Number(0), ee.List([])])
    result = ee.List(sorted_imgs.iterate(accumulate, init))

    final_imgs = ee.List(result.get(1))
    print("📸 Выбрано снимков:", final_imgs.size().getInfo())

    # Маскируем облака только после отбора
    final_collection = ee.ImageCollection.fromImages(final_imgs.map(lambda img: mask_clouds(img)))

    mosaic = final_collection.mosaic().clip(region)
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
            .map(lambda img: img.resample("bicubic").select(["TCI_R", "TCI_G", "TCI_B", "SCL"]))
        )

        total = raw_collection.size().getInfo()
        print(f"📥 Доступно снимков: {total}")

        mosaic = build_mosaic_with_coverage(raw_collection, geometry)

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
