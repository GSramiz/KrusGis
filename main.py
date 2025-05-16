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

def build_mosaic_with_coverage(collection, geometry, min_coverage=0.95):
    masked_collection = collection.map(mask_clouds)
    imgs = masked_collection.toList(masked_collection.size())
    total_area = geometry.area()

    # Начальное состояние: пустой список выбранных изображений и пустая маска покрытия
    initial = {
        'selected': ee.List([]),
        'coverage_mask': ee.Image(0).clip(geometry).mask().rename('mask')
    }

    def iter_fun(i, acc):
        acc = ee.Dictionary(acc)
        selected = ee.List(acc.get('selected'))
        coverage_mask = ee.Image(acc.get('coverage_mask'))

        img = ee.Image(imgs.get(i))
        img_mask = img.mask().rename('mask')

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
            return {
                'selected': selected.add(img),
                'coverage_mask': new_coverage_mask
            }

        def skip_img():
            return {
                'selected': selected,
                'coverage_mask': coverage_mask
            }

        # Если покрытие < min_coverage — добавляем снимок, иначе пропускаем
        return ee.Algorithms.If(
            coverage_ratio.lt(min_coverage),
            add_img(),
            skip_img()
        )

    final_acc = ee.Dictionary(ee.List.sequence(0, imgs.size().subtract(1)).iterate(iter_fun, initial))
    selected_imgs = final_acc.get('selected')
    coverage_mask_final = ee.Image(final_acc.get('coverage_mask'))

    final_collection = ee.ImageCollection.fromImages(selected_imgs)
    mosaic = final_collection.mosaic().clip(geometry)

    coverage_area_dict = coverage_mask_final.reduceRegion(
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
            .limit(100)
        )

        count = raw_collection.size().getInfo()
        if count == 0:
            print("❌ Нет снимков за указанный период")
            return

        print(f"📥 Доступно снимков: {count}")

        mosaic, coverage = build_mosaic_with_coverage(raw_collection, geometry, min_coverage=0.95)

        # Количество снимков приблизительно — делим количество бэндов на 4 (TCI_R,G,B,SCL)
        bands_count = mosaic.bandNames().size().getInfo()
        print(f"📸 Выбрано снимков: {bands_count // 4}")

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
