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
    try:
        fc = ee.FeatureCollection("projects/ee-romantik1994/assets/region")
        region = fc.filter(ee.Filter.eq("title", region_name)).first()
        if region is None:
            raise ValueError(f"Регион '{region_name}' не найден в ассете")
        return region.geometry()
    except Exception as e:
        log_error("get_geometry_from_asset", e)
        raise

def mask_clouds(img):
    scl = img.select("SCL")
    cloud_mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    return img.updateMask(cloud_mask)

def get_footprint_coverage(image, region_geom):
    # Получаем маску, чтобы считать покрытие снимка по региону
    footprint = image.geometry().intersection(region_geom, 1)
    footprint_area = footprint.area(1)
    region_area = region_geom.area(1)
    # Вернуть отношение покрытой площади (число ee.Number)
    return footprint_area.divide(region_area)

def build_mosaic_by_coverage(collection, region, min_coverage=0.95):
    sorted_imgs = collection.sort("CLOUDY_PIXEL_PERCENTAGE")

    def accumulate(img, acc):
        coverage_so_far = ee.Number(ee.List(acc).get(0))
        imgs_so_far = ee.List(ee.List(acc).get(1))

        cov = get_footprint_coverage(img, region)
        new_coverage = coverage_so_far.add(cov)

        should_add = new_coverage.lt(min_coverage)

        updated_imgs = ee.Algorithms.If(
            should_add,
            imgs_so_far.add(img),
            imgs_so_far
        )

        updated_cov = ee.Algorithms.If(
            should_add,
            new_coverage,
            coverage_so_far
        )

        return ee.List([updated_cov, updated_imgs])

    init = ee.List([ee.Number(0), ee.List([])])

    result = sorted_imgs.iterate(accumulate, init)

    coverage_final = ee.List(result).get(0)
    images_final = ee.List(result).get(1)

    final_collection = ee.ImageCollection(images_final)
    mosaic = final_collection.mosaic().resample("bicubic").clip(region)

    return mosaic

def main():
    try:
        initialize_services()

        region_name = "Алтайский край"
        start_date = "2022-05-01"
        end_date = "2022-06-01"

        print(f"\n🗺️ Регион: {region_name}, период: {start_date} → {end_date}")
        geometry = get_geometry_from_asset(region_name)

        collection = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterDate(start_date, end_date)
            .filterBounds(geometry)
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 40))
            .map(mask_clouds)
            .map(lambda img: img.resample("bicubic").select(["TCI_R", "TCI_G", "TCI_B"]))
            # Сортировка по облачности ниже будет в accumulate
        )

        count = collection.size().getInfo()
        if count == 0:
            print("❌ Нет снимков за указанный период")
            return

        print(f"📸 Используем снимков: {count}")

        mosaic = build_mosaic_by_coverage(collection, geometry, min_coverage=0.95)

        vis_params = {"bands": ["TCI_R", "TCI_G", "TCI_B"], "min": 0, "max": 255}

        tile_info = ee.data.getMapId({
            "image": mosaic,
            "visParams": vis_params
        })
        mapid = tile_info["mapid"].split("/")[-1]
        xyz_url = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{mapid}/tiles/{{z}}/{{x}}/{{y}}"

        print("\n✅ Мозаика построена. XYZ-ссылка:")
        print(xyz_url)

    except Exception as e:
        log_error("main", e)

if __name__ == "__main__":
    main()
