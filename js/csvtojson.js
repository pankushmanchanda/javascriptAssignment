var fs = require("fs");
var readStream = fs.createReadStream("../data_source/FoodFacts.csv", "UTF-8");
var byline = require("byline");
readStream = byline.createStream(readStream);
var length = 0;
var headers;
var json = '';
var result = [];
var writeStream = fs.createWriteStream("../data_source/myjson.txt");
readStream.on('data', function(line) {
  //Reading file through stream and getting the required data from CSV starts
  if (length < 1) {
    headers = line.toString().split(","); //header of csv on first iteration
  } else {

    var colVal = line.toString().split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/); //spliting line of file with regex even comma lies between columns values
    var obj = {};
    var indexCountries = headers.indexOf("countries_en"); //index of CSV columns by name starts
    var indexSugar = headers.indexOf("sugars_100g");
    var indexSalt = headers.indexOf("salt_100g");
    var indexFat = headers.indexOf("fat_100g");
    var indexProtein = headers.indexOf("proteins_100g");
    var indexCarbo = headers.indexOf("carbohydrates_100g");
    var indexDate = headers.indexOf("last_modified_datetime"); //index of CSV columns by name end
    obj["Country"] = colVal[indexCountries].replace(/["']/g, "");
    obj["Sugar"] = colVal[indexSugar];
    obj["Salt"] = colVal[indexSalt];
    obj["Fat"] = colVal[indexFat];
    obj["Protein"] = colVal[indexProtein];
    obj["Carbohydrate"] = colVal[indexCarbo];
    obj["Date"] = colVal[indexDate];
    result.push(obj);
  }
  length++;
}).on('end', function() {
  //Applying filter on data after reading the file
  var consumeArray = result.filter(function(item) {
    return (item.Sugar != "" || item.Salt != "" || item.Country == "Netherlands" || item.Country == "France" || item.Country == "United Kingdom" || item.Country == "Germany" || item.Country == "Canada" || item.Country == "Spain" || item.Country == "United States" || item.Country == "Australia" || item.Country == "South Africa") && !(item.Fat) && !(item.Protein) && !(item.Carbohydrate) && !(item.Date);
  });
  var regionArray = result.filter(function(item) {
    return (item.Fat != "" || item.Protein != "" || item.Carbohydrate != "") && !(item.Sugar) && !(item.Salt);
  });

  // var filterConsumeArray = filterbyCountry(consumeArray, 'Country', 'Sugar', 'Salt');

  var filteregionArray = filterbyRegion(regionArray, 'Country', 'Date', 'Fat', 'Protein', 'Carbohydrate');
  writeStream.write(JSON.stringify(filteregionArray));
  writeStream.end();
  //Reading file through stream and getting the required data from CSV starts

});

//sum of salt and sugar per country
function filterbyCountry(array, countryCol, sumSugar, sumSalt) {
  var filterResult = [],
    newObj = {};
  array.forEach(function(argPass) {

    if (!newObj[argPass[countryCol]]) {
      newObj[argPass[countryCol]] = {};
      newObj[argPass[countryCol]][countryCol] = argPass[countryCol];
      newObj[argPass[countryCol]][sumSugar] = 0;
      newObj[argPass[countryCol]][sumSalt] = 0;
      filterResult.push(newObj[argPass[countryCol]]);
    }
    newObj[argPass[countryCol]][sumSugar] += +argPass[sumSugar];
    newObj[argPass[countryCol]][sumSalt] += +argPass[sumSalt];
  });
  return filterResult;
};

function filterbyRegion(array, countryCol, dateTime, sumFat, sumProtein, sumCarbohydrate) {
  var filterResult = [],
    newObj = {};
  var region = 'Region';
  var regionNorth = array.filter(function(item) {
    return (item.Country == "United Kingdom" || item.Country == "Denmark" || item.Country == "Sweden" || item.Country == "Norway");
  });
  var regionCenter = array.filter(function(item) {
    return (item.Country == "France" || item.Country == "Belgium" || item.Country == "Germany" || item.Country == "Switzerland" || item.Country == "Netherlands");
  });
  var regionSouth = array.filter(function(item) {
    return (item.Country == "Portugal" || item.Country == "Greece" || item.Country == "Italy" || item.Country == "Spain" || item.Country == "Croatia" || item.Country == "Albania");
  });

  regionNorth.forEach(function(argPass) {

    if (!newObj[argPass[dateTime]]) {
      newObj[argPass[dateTime]] = {};
      newObj[argPass[dateTime]][region] = 'NorthEurope';
      newObj[argPass[dateTime]][dateTime] = argPass[dateTime];
      newObj[argPass[dateTime]][sumFat] = 0;
      newObj[argPass[dateTime]][sumProtein] = 0;
      newObj[argPass[dateTime]][sumCarbohydrate] = 0;
      filterResult.push(newObj[argPass[dateTime]]);
    }
    newObj[argPass[dateTime]][sumFat] += +argPass[sumFat];
    newObj[argPass[dateTime]][sumProtein] += +argPass[sumProtein];
    newObj[argPass[dateTime]][sumCarbohydrate] += +argPass[sumCarbohydrate];
  });
  regionCenter.forEach(function(argPass) {

    if (!newObj[argPass[dateTime]]) {
      newObj[argPass[dateTime]] = {};
      newObj[argPass[dateTime]][region] = 'CenterEurope';
      newObj[argPass[dateTime]][dateTime] = argPass[dateTime];
      newObj[argPass[dateTime]][sumFat] = 0;
      newObj[argPass[dateTime]][sumProtein] = 0;
      newObj[argPass[dateTime]][sumCarbohydrate] = 0;
      filterResult.push(newObj[argPass[dateTime]]);
    }
    newObj[argPass[dateTime]][sumFat] += +argPass[sumFat];
    newObj[argPass[dateTime]][sumProtein] += +argPass[sumProtein];
    newObj[argPass[dateTime]][sumCarbohydrate] += +argPass[sumCarbohydrate];
  });
  regionSouth.forEach(function(argPass) {

    if (!newObj[argPass[dateTime]]) {
      newObj[argPass[dateTime]] = {};
      newObj[argPass[dateTime]][region] = 'SouthEurope';
      newObj[argPass[dateTime]][dateTime] = argPass[dateTime];
      newObj[argPass[dateTime]][sumFat] = 0;
      newObj[argPass[dateTime]][sumProtein] = 0;
      newObj[argPass[dateTime]][sumCarbohydrate] = 0;
      filterResult.push(newObj[argPass[dateTime]]);
    }
    newObj[argPass[dateTime]][sumFat] += +argPass[sumFat];
    newObj[argPass[dateTime]][sumProtein] += +argPass[sumProtein];
    newObj[argPass[dateTime]][sumCarbohydrate] += +argPass[sumCarbohydrate];
  });
  return filterResult;
};
