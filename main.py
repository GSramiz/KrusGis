// 📌 Подключение к Google Таблице и коллекции регионов
var table = ee.FeatureCollection("projects/ee-romantik1994/assets/region");

// 📅 Массив месяцев и преобразование в дату
var monthMap = {
  'январь': '01', 'февраль': '02', 'март': '03', 'апрель': '04',
  'май': '05', 'июнь': '06', 'июль': '07', 'август': '08',
  'сентябрь': '09', 'октябрь': '10', 'ноябрь': '11', 'декабрь': '12'
};

// 📥 Импорт таблицы из Google Sheets
var sheet = 'Sentinel-2 Покрытие';
var spreadsheetId = '1oz12JnCKuM05PpHNR1gkNR_tPENazabwOGkWWeAc2hY';
var range = 'A2:C821';

// 🔁 Обработка каждой строки с клиентской стороны
function generateUrls() {
  var sheet = SpreadsheetApp.openById(spreadsheetId).getSheetByName('Sentinel-2 Покрытие');
  var data = sheet.getRange(range).getValues();
  
  for (var i = 0; i < data.length; i++) {
    var regionName = data[i][0];
    var monthYear = data[i][1];

    if (!regionName || !monthYear) continue;

    var parts = monthYear.toLowerCase().split(" ");
    var month = monthMap[parts[0]];
    var year = parts[1];
    var start = ee.Date(year + '-' + month + '-01');
    var end = start.advance(1, 'month');

    var region = table.filter(ee.Filter.eq('title', regionName)).geometry();

    var vis = {
      bands: ['TCI_R', 'TCI_G', 'TCI_B'],
      min: 0,
      max: 255
    };

    var collection = ee.ImageCollection("COPERNICUS/S2_SR")
      .filterDate(start, end)
      .filterBounds(region)
      .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 60))
      .map(function(img) {
        return img.select(['TCI_R', 'TCI_G', 'TCI_B'])
                  .resample('bicubic')
                  .copyProperties(img, img.propertyNames());
      });

    var mosaic = collection.mosaic().clip(region);
    var kernel = ee.Kernel.gaussian({ radius: 1.2, sigma: 1.2, units: 'pixels', normalize: true });
    var smoothed = mosaic.convolve(kernel);

    Map.addLayer(smoothed, vis, "🛰 " + regionName + " (" + monthYear + ")");

    var map = smoothed.visualize(vis).getMap();
    var mapid = map.mapid;
    var token = map.token;
    var xyzUrl = 'https://earthengine.googleapis.com/v1/projects/ee-romantik1994/maps/' + mapid + '/tiles/{z}/{x}/{y}';

    // ✍️ Запись URL обратно в таблицу
    sheet.getRange(i + 2, 3).setValue(xyzUrl);
  }
}

generateUrls();
