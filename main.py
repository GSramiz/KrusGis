import ee
import json

def initialize_services():
    try:
        with open("service-account.json") as f:
            service_account_info = json.load(f)

        credentials = ee.ServiceAccountCredentials(service_account_info["client_email"], "service-account.json")
        ee.Initialize(credentials)
        print("✅ EE инициализирован через сервисный аккаунт")
    except Exception as e:
        print("❌ Ошибка инициализации EE:", e)
        raise

def get_geometry_from_asset(region_name):
    try:
        fc = ee.FeatureCollection("projects/ee-romantik1994/assets/region")
        region = fc.filter(ee.Filter.eq("title", region_name)).first()
        if region is None:
            raise ValueError(f"Регион '{region_name}' не найден в ассете")
        return region.geometry()
    except Exception as e:
        print("❌ Ошибка get_geometry_from_asset:", e)
        raise

def mask_clouds(img):
    scl = img.select("SCL")
    mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    return img.updateMask(mask)

def get_footprint_coverage(img, region):
    img_geom = img.geometry()
    intersection = img_geom.intersection(region, 1)
    inter_area = intersection.area(1)
    region_area = region.area(1)
    coverage = inter_area.divide(region_area)
    return coverage

def build_mosaic_by_coverage(collection, region, min_coverage=0.95):
    # Сортируем коллекцию по облачности
    sorted_imgs = collection.sort("CLOUDY_PIXEL_PERCENTAGE")

    def accumulate(img, acc):
        acc = ee.List(acc)
        coverage_so_far = ee.Number(acc.get(0))
        imgs_so_far = ee.List(acc.get(1))

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

    result = ee.List(sorted_imgs.iterate(accumulate, init))

    coverage_final = ee.Number(result.get(0))
    images_final = ee.List(result.get(1))

    print("✅ Итоговое покрытие:", coverage_final.getInfo())
    print("✅ Выбранных снимков:", images_final.size().getInfo())

    final_collection = ee.ImageCollection.fromImages(images_final)
    mosaic = final_collection.mosaic().resample("bicubic").clip(region)
    mosaic_filled = mosaic.unmask(0)  # Заполняем прозрачные пиксели нулями
    
    return mosaic_filled

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
            .sort("CLOUDY_PIXEL_PERCENTAGE")  # Сортируем до маскировки
            .map(mask_clouds)                 # Маскируем облака после сортировки
            .map(lambda img: img.resample("bicubic").select(["TCI_R", "TCI_G", "TCI_B"]))
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
        xyz_url = f"https://earthengine.googleapis.com/v1/projects/earthengine-legacy/maps/{mapid}/tiles/{{z}}/{{x}}/{{y}}"

        print("\n✅ Мозаика построена. XYZ-ссылка:")
        print(xyz_url)

    except Exception as e:
        print("❌ Ошибка в main:", e)

if __name__ == "__main__":
    main()
