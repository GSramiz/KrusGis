def update_sheet(sheets_client):
    try:
        print("Обновление таблицы")

        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        data = worksheet.get_all_values()

        geometry_cache = {}

        for row_idx, row in enumerate(data[1:], start=2):
            try:
                region, date_str = row[:2]
                if not region or not date_str:
                    continue

                parts = date_str.strip().split()
                if len(parts) != 2:
                    raise ValueError(f"Неверный формат даты: '{date_str}'")

                month_num = month_str_to_number(parts[0])
                year = parts[1]
                start = f"{year}-{month_num}-01"
                days = calendar.monthrange(int(year), int(month_num))[1]
                end_str = f"{year}-{month_num}-{days:02d}"

                print(f"\n {region} — {start} - {end_str}")

                if region in geometry_cache:
                    geometry = geometry_cache[region]
                else:
                    geometry = get_geometry_from_asset(region)
                    geometry_cache[region] = geometry

                collection = (
                    ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                    .filterDate(start, end_str)
                    .filterBounds(geometry)
                    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30))
                    .map(mask_clouds)
                )

                if collection.first().getInfo() is None:
                    worksheet.update_cell(row_idx, 3, "Нет снимков")
                    continue

                filtered_mosaic = collection.mosaic()

                tile_info = ee.data.getMapId({
                    "image": filtered_mosaic,
                    "bands": ["B4", "B3", "B2"],
                    "min": "0,0,0",
                    "max": "3000,3000,3000"
                })

                clean_mapid = tile_info["mapid"].split("/")[-1]
                xyz = f"https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/{clean_mapid}/tiles/{{z}}/{{x}}/{{y}}"

                worksheet.update_cell(row_idx, 3, xyz)

            except Exception as e:
                log_error(f"Строка {row_idx}", e)
                worksheet.update_cell(row_idx, 3, f"Ошибка: {str(e)[:100]}")

    except Exception as e:
        log_error("update_sheet", e)
        raise
