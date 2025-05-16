import ee
import json
import os
import traceback

def log_error(context, error):
    print(f"\n❌ ОШИБКА в {context}:")
    print(f"Тип: {type(error).__name__}")
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

def build_optimal_mosaic(region_name, start, end, min_coverage=0.95):
    try:
        print(f"\n🗺️ Регион: {region_name}, период: {start} → {end}")
        geometry = get_geometry_from_asset(region_name)

        # Получаем полную коллекцию
        collection = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterDate(start, end)
            .filterBounds(geometry)
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 40))
            .map(mask_clouds)
            .map(lambda img: img.resample("bicubic").select(["TCI_R", "TCI_G", "TCI_B"]))
            .sort("CLOUDY_PIXEL_PERCENTAGE")
        )

        # Переходим к покрытию
        def accumulate_images(image_list, geom, min_area):
            total = ee.Image(0).updateMask(ee.Image(0).mask())  # пустая
            area = 0
            index = 0
            while_area = ee.Number(0)
            result_list = []

            while True:
                img = ee.Image(image_list.get(index))
                total = total.unmask().blend(img)
                covered = total.mask().reduceRegion(
                    reducer=ee.Reducer.sum(),
                    geometry=geom,
                    scale=20,
                    maxPixels=1e9
                ).get("TCI_R")
                area = ee.Number(covered)
                if area.divide(geom.area()).getInfo() >= min_area:
                    result_list.append(img)
                    break
                result_list.append(img)
                index += 1
            return ee.ImageCollection(result_list)

        image_list = collection.toList(collection.size())
        geom_area = geometry.area()
        optimal_images = accumulate_images(image_list, geometry, min_coverage)
        print("📸 Используем снимков:", optimal_images.size().getInfo())

        mosaic = optimal_images.mosaic().clip(geometry)

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
        log_error("build_optimal_mosaic", e)

if __name__ == "__main__":
    try:
        initialize_services()
        build_optimal_mosaic(
            region_name="Алтайский край",
            start="2022-05-01",
            end="2022-06-01"
        )
    except Exception as e:
        log_error("main", e)
        exit(1)
