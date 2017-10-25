var querystring = require('querystring');
var http = require('http');
var fs = require('fs');
const Lokka = require('lokka').Lokka;
const Transport = require('lokka-transport-http').Transport;

const headers = {
    'accept': 'application/json'
};

const mutationQuery = '($input: CreateTrafficSplitInput!) { createTrafficSplit(input: $input) { experimentUuid } }';

function PostCode(trafficSplitData) {
  const graphQLEndPoint = process.env.GRAPHQL_END_POINT || 'https://analysis-api.distillery.dx.spectrumtoolbox.com/graphql?';
  const client = new Lokka({
    transport: new Transport(graphQLEndPoint, {headers: headers})
  });

  const trafficSplitArray = JSON.parse(trafficSplitData);
  trafficSplitArray.forEach(function(trafficSplitEntry) {
    const vars = { input: trafficSplitEntry };
    console.log('Posting trafficSplitEntry ', vars)
    client.mutate(mutationQuery, vars).catch(function (err) {
       console.log("Promise Rejected ", err);
     }).then(function(resp) {
        console.log('Response from mutation ', resp);
    });
  });
}

function transformCsvToTrafficSplitData(data) {
  const textByLine = data.split("\n")
  const trafficSplitEntries = [];
  var expUuid;
  var varUuidArray = [];
  var numOfVariants;

  textByLine.forEach(function (line, index) {
    const lineEntries = line.split(",");
    if (index == 0) {
      console.log('Header line  ', line)
      expUuid = lineEntries[2];
      numOfVariants = lineEntries.length - 6;

      console.log('Number of Variants ', numOfVariants)
      for (var variantIndex = 0; variantIndex < numOfVariants; variantIndex++) {
        console.log('Variant line entry ', lineEntries[variantIndex + 3]);
        varUuidArray.push(lineEntries[variantIndex + 3]);
      }
      console.log('variantArray ', varUuidArray)
    } else {
      const measuredAt = lineEntries[1];
      const var1Percentage = lineEntries[6];
      const var2Percentage = lineEntries[7];

      if (measuredAt) {
        // console.log('treatment allocations ', measuredAt, var1Percentage, var2Percentage)
        const trafficSplitEntry = {};
        trafficSplitEntry.experimentUuid = expUuid;
        trafficSplitEntry.measuredAt = measuredAt;

        const trafficSplitDataEntries = [];

        for (var variantIndex = 0; variantIndex < numOfVariants; variantIndex++) {
          var varEntry = {};
          varEntry.variantUuid = varUuidArray[variantIndex];
          varEntry.percentage = lineEntries[ 4 + numOfVariants + variantIndex].trim();
          trafficSplitDataEntries.push(varEntry);
        }

        trafficSplitEntry.data = trafficSplitDataEntries;
        trafficSplitEntries.push(trafficSplitEntry);
      }
    }
  });
  return JSON.stringify(trafficSplitEntries);
}

const analysisCSVFileName = process.argv.slice(2)[0] || 'SampleData.csv';
console.log('Loading data from ', analysisCSVFileName)
// This is an async file read
fs.readFile(analysisCSVFileName, 'utf-8', function (err, data) {
  if (err) {
    // If this were just a small part of the application, you would
    // want to handle this differently, maybe throwing an exception
    // for the caller to handle. Since the file is absolutely essential
    // to the program's functionality, we're going to exit with a fatal
    // error instead.
    console.log("FATAL An error occurred trying to read in the file: " + err);
    process.exit(-2);
  }
  // Make sure there's data before we post it
  if(data) {
    const trafficSplitData = transformCsvToTrafficSplitData(data);
    console.log(trafficSplitData)
    PostCode(trafficSplitData);
  }
  else {
    console.log("No data to post");
    process.exit(-1);
  }
});
