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
    print("\n🔧 Инициализация Earth Engine...")
    service_account_info = json.loads(os.environ["GEE_CREDENTIALS"])
    credentials = ee.ServiceAccountCredentials(
        service_account_info["client_email"],
        key_data=json.dumps(service_account_info)
    )
    ee.Initialize(credentials)
    print("✅ EE инициализирован")

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

def build_optimal_mosaic(region_name, start, end, coverage_threshold=0.9):
    geometry = get_geometry_from_asset(region_name)

    # Получаем изображения, маскируем облака
    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterDate(start, end)
        .filterBounds(geometry)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 40))
        .map(mask_clouds)
        .map(lambda img: img.clip(geometry).resample("bicubic").select(["TCI_R", "TCI_G", "TCI_B"]).set("system:time_start", img.date().millis()))
    )

    # Получаем площадь региона в м²
    region_area = geometry.area().divide(1e6)  # в км²

    # Вычисляем покрытие каждого снимка
    def compute_coverage(img):
        mask = img.mask().select(0)
        covered_area = mask.multiply(ee.Image.pixelArea()).reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=geometry,
            scale=20,
            maxPixels=1e10
        ).get("TCI_R")
        return img.set("covered_area", covered_area)

    collection = collection.map(compute_coverage)

    # Преобразуем в список
    imgs = collection.toList(collection.size())

    def accumulate(img, state):
        img = ee.Image(img)
        state = ee.Dictionary(state)
        used = ee.List(state.get("used"))
        total = ee.Number(state.get("total"))
        area = ee.Number(img.get("covered_area"))

        new_total = total.add(area)
        new_used = used.add(img)

        # Прерываем, если достигли покрытия
        return ee.Algorithms.If(
            new_total.divide(region_area.multiply(1e6)).lte(coverage_threshold),
            ee.Dictionary({"used": new_used, "total": new_total}),
            ee.Dictionary({"used": used, "total": total})  # не добавляем
        )

    # Инициализируем словарь
    init = ee.Dictionary({"used": ee.List([]), "total": 0})
    result = imgs.iterate(accumulate, init)
    used_images = ee.List(ee.Dictionary(result).get("used"))

    # Мозаика по выбранным снимкам
    mosaic = ee.ImageCollection(used_images).mosaic().clip(geometry)

    # Визуализация
    vis = {"bands": ["TCI_R", "TCI_G", "TCI_B"], "min": 0, "max": 255}
    tile_info = ee.data.getMapId({"image": mosaic, "visParams": vis})
    xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{tile_info['mapid'].split('/')[-1]}/tiles/{{z}}/{{x}}/{{y}}"

    print(f"\n✅ Мозаика построена. XYZ-ссылка:\n{xyz}")

if __name__ == "__main__":
    try:
        initialize_services()
        build_optimal_mosaic(
            region_name="Алтайский край",
            start="2022-05-01",
            end="2022-06-01",
            coverage_threshold=0.9  # 90% покрытия
        )
    except Exception as e:
        log_error("main", e)
        exit(1)
