import os
import ee
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Authenticate with Earth Engine using service account
service_account = os.environ.get("EE_SERVICE_ACCOUNT")
private_key = os.environ.get("EE_PRIVATE_KEY")
credentials = ee.ServiceAccountCredentials(service_account, key_data=private_key)
ee.Initialize(credentials)

# Authenticate with Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
sheets_credentials = ServiceAccountCredentials.from_json_keyfile_name("gdrive_key.json", scope)
client = gspread.authorize(sheets_credentials)

# Open spreadsheet and worksheet
spreadsheet = client.open("Sentinel Tiles")
worksheet = spreadsheet.sheet1

# Define parameters
START_DATE = '2022-05-01'
END_DATE = '2022-05-31'
REGION = ee.Geometry.BBox(37.5, 55.5, 38.0, 56.0)
TILE_SCALE = 100

# Define tile grid
def generate_tiles(region, dx=0.25, dy=0.25):
    coords = region.bounds().coordinates().get(0)
    coords = ee.List(coords)
    xmin = ee.Number(ee.List(coords.get(0)).get(0))
    ymin = ee.Number(ee.List(coords.get(0)).get(1))
    xmax = ee.Number(ee.List(coords.get(2)).get(0))
    ymax = ee.Number(ee.List(coords.get(2)).get(1))

    def make_tile(xi, yi):
        x0 = xmin.add(dx * xi)
        x1 = x0.add(dx)
        y0 = ymin.add(dy * yi)
        y1 = y0.add(dy)
        return ee.Feature(ee.Geometry.BBox(x0, y0, x1, y1))

    nx = xmax.subtract(xmin).divide(dx).ceil().toInt()
    ny = ymax.subtract(ymin).divide(dy).ceil().toInt()

    tiles = []
    for i in range(nx.getInfo()):
        for j in range(ny.getInfo()):
            tiles.append(make_tile(i, j))
    return ee.FeatureCollection(tiles)

tiles = generate_tiles(REGION)

# Cloud mask function using SCL band
def mask_scl(image):
    scl = image.select('SCL')
    mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    return image.updateMask(mask)

# Visualize RGB (TCI bands)
def toRGB(image):
    rgb = image.select(['TCI_R', 'TCI_G', 'TCI_B']).divide(255)
    return rgb.copyProperties(image, image.propertyNames())

# Process each tile
def process_tile(tile):
    geometry = tile.geometry()

    collection = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                  .filterBounds(geometry)
                  .filterDate(START_DATE, END_DATE)
                  .map(mask_scl)
                  .map(toRGB))

    # Сначала создаём черновую мозаику
    initial_mosaic = collection.mosaic()
    mosaic_mask = initial_mosaic.mask().reduce(ee.Reducer.anyNonZero())

    def contributes(image):
        image_mask = image.mask().reduce(ee.Reducer.anyNonZero())
        combined = image_mask.And(mosaic_mask)
        overlap = combined.reduceRegion(
            reducer=ee.Reducer.anyNonZero(),
            geometry=geometry,
            scale=100,
            maxPixels=1e6
        ).values().get(0)
        return image.set("contributes", overlap)

    filtered = collection.map(contributes).filter(ee.Filter.eq("contributes", 1))
    final_mosaic = filtered.mosaic()

    url = final_mosaic.clip(geometry).getMapId({"min": 0, "max": 1})["tile_fetcher"].url_format

    centroid = geometry.centroid().coordinates()
    lon = centroid.get(0).getInfo()
    lat = centroid.get(1).getInfo()
    return [f"{lat:.4f}, {lon:.4f}", url]

# Clear worksheet
worksheet.clear()
worksheet.append_row(["Tile Center (Lat, Lon)", "XYZ URL"])

# Process all tiles and append to sheet
tile_list = tiles.toList(tiles.size())
for i in range(tiles.size().getInfo()):
    tile = ee.Feature(tile_list.get(i))
    try:
        row = process_tile(tile)
        worksheet.append_row(row)
    except Exception as e:
        print(f"Failed to process tile {i}: {e}")
