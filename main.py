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
    cloud_mask = cloud_mask.rename('mask')  # Обязательно 1 бэнд
    return img.updateMask(cloud_mask)

def build_mosaic_with_coverage(collection, geometry, min_coverage=0.95):
    masked_collection = collection.map(mask_clouds)
    imgs = masked_collection.toList(masked_collection.size())

    total_area = geometry.area()

    def iter_fun(i, acc):
        acc = ee.List(acc)
        coverage_mask = ee.Image(acc.get(0))
        selected_imgs = ee.List(acc.get(1))

        img = ee.Image(imgs.get(i))

        # Приводим маску к однобандовой с названием 'mask'
        img_mask = img.mask().reduce(ee.Reducer.min()).rename('mask')

        new_coverage_mask = coverage_mask.Or(img_mask).rename('mask')

        coverage_area_dict = new_coverage_mask.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geometry,
            scale=10,
            maxPixels=1e13
        )
        coverage_area = ee.Number(coverage_area_dict.get('mask'))
        coverage_ratio = coverage_area.divide(total_area)

        def add_img():
            return ee.List([new_coverage_mask, selected_imgs.add(img)])

        def skip_img():
            return ee.List([coverage_mask, selected_imgs])

        return ee.Algorithms.If(
            coverage_ratio.lt(min_coverage),
            add_img(),
            skip_img()
        )

    # Начальная маска покрытия — однобандовое изображение 0, названное 'mask'
    empty_mask = ee.Image(0).rename('mask').clip(geometry)

    init_acc = ee.List([empty_mask, ee.List([])])

    result = ee.List(ee.List.sequence(0, imgs.size().subtract(1)).iterate(iter_fun, init_acc))

    final_coverage_mask = ee.Image(result.get(0))
    selected_images = ee.List(result.get(1))

    final_collection = ee.ImageCollection.fromImages(selected_images)

    mosaic = final_collection.mosaic().clip(geometry)

    # Итоговое покрытие для отчёта
    coverage_area_dict = final_coverage_mask.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geometry,
        scale=10,
        maxPixels=1e13
    )
    coverage_area = ee.Number(coverage_area_dict.get('mask'))
    coverage_ratio = coverage_area.divide(total_area)

    return mosaic, coverage_ratio

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
            .sort("CLOUDY_PIXEL_PERCENTAGE")
            .limit(100)  # Чтобы не брать слишком много сразу
        )

        count = raw_collection.size().getInfo()
        if count == 0:
            print("❌ Нет снимков за указанный период")
            return

        print(f"📥 Доступно снимков: {count}")

        mosaic, coverage = build_mosaic_with_coverage(raw_collection, geometry, min_coverage=0.95)

        # Получаем число выбранных снимков (просто длина списка выбранных изображений)
        selected_count = mosaic.bandNames().size().getInfo() // 3  # Здесь 3 бэнда: TCI_R,G,B
        print(f"📸 Приблизительно выбрано снимков: {selected_count}")

        vis = {"bands": ["TCI_R", "TCI_G", "TCI_B"], "min": 0, "max": 255}
        tile_info = ee.data.getMapId({
            "image": mosaic,
            "visParams": vis
        })
        mapid = tile_info["mapid"].split("/")[-1]
        xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{mapid}/tiles/{{z}}/{{x}}/{{y}}"

        print("\n✅ Мозаика построена. XYZ-ссылка:")
        print(xyz)

        print("✅ Итоговое покрытие:", coverage.getInfo())

    except Exception as e:
        log_error("test_mosaic_region", e)

if __name__ == "__main__":
    try:
        initialize_services()
        test_mosaic_region()
    except Exception as e:
        log_error("main", e)
        exit(1)
