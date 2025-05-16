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

def build_mosaic_with_coverage(collection, region, min_coverage=0.95):
    sorted_imgs = collection.sort("CLOUDY_PIXEL_PERCENTAGE")
    region_area = region.area(1)

    def accumulate(img, acc):
        acc = ee.List(acc)
        coverage_so_far = ee.Number(acc.get(0))
        imgs_so_far = ee.List(acc.get(1))

        # Пересечение снимка с регионом
        intersection = img.geometry().intersection(region, 1)
        intersection_area = intersection.area(1)

        # Получаем облачность из метаданных
        cloudy = ee.Number(img.get('CLOUDY_PIXEL_PERCENTAGE'))

        # Чистая площадь снимка на территории региона
        clear_area = intersection_area.multiply(ee.Number(1).subtract(cloudy.divide(100)))

        # Новое покрытие с учетом этого снимка
        new_coverage = coverage_so_far.add(clear_area.divide(region_area))

        should_add = new_coverage.lt(min_coverage)

        updated_imgs = ee.Algorithms.If(should_add, imgs_so_far.add(img), imgs_so_far)
        updated_cov = ee.Algorithms.If(should_add, new_coverage, coverage_so_far)

        return ee.List([updated_cov, updated_imgs])

    result = ee.List(sorted_imgs.iterate(accumulate, ee.List([ee.Number(0), ee.List([])])))
    selected = ee.List(result.get(1))

    print(f"📸 Выбрано снимков: {selected.size().getInfo()}")

    selected_ic = ee.ImageCollection.fromImages(selected)
    masked = selected_ic.map(mask_clouds).map(lambda img: img.resample("bicubic").select(["TCI_R", "TCI_G", "TCI_B"]))

    mosaic = masked.mosaic().clip(region)
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

        mosaic = build_mosaic_with_coverage(raw_collection, geometry, min_coverage=0.95)

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
