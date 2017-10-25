var querystring = require('querystring');
var http = require('http');
var fs = require('fs');
const Lokka = require('lokka').Lokka;
const Transport = require('lokka-transport-http').Transport;

const headers = {
    'accept': 'application/json'
    //,'Authorization': 'prod-token'
};

const mutationQuery = '($metrics:  CreateMetricsInput!) { createMetrics(metrics: $metrics) }';

function PostCode(metricsJson) {
  const graphQLEndPoint = process.env.GRAPHQL_END_POINT || 'https://analysis-api.distillery.dx.spectrumtoolbox.com/graphql?';
  const client = new Lokka({
    transport: new Transport(graphQLEndPoint, {headers: headers})
  });

  const metrics = JSON.parse(metricsJson);
  // console.log('Posting Metrics ', {metrics})

  client.mutate(mutationQuery, {metrics: metrics}).catch(function (err) {
     console.log("Promise Rejected ", err);
   })
  //  .then(resp =>
  //     console.log('Response from mutation ', resp);
  // })
  ;
}

function getCustomerCounts(err, resultsData) {
  const customerCounts = [];
  if (err) {
    console.log("FATAL An error occurred trying to read in the file: " + err);
    process.exit(-2);
  }
  // Make sure there's data before we post it
  if(resultsData) {
    const textByLine = resultsData.split("\n")

    var numOfVariants;

    if ( textByLine.length >= 2) {
      var variantEntries = textByLine[0].split(",");
      var variantCountEntries = textByLine[1].split(",");

      numOfVariants = variantEntries.length - 3;

      for (var variantIndex = 0; variantIndex < numOfVariants; variantIndex++) {
        var customerCountEntry = {};
        customerCountEntry.uuid = variantEntries[variantIndex + 3].trim();
        customerCountEntry.value = variantCountEntries[variantIndex + 3].trim();
        customerCounts.push(customerCountEntry);
      }
      console.log('customerCounts ', customerCounts);
    };
  };
  return customerCounts;
}

function transformCsvToMetricsMap(data) {
  const textByLine = data.split("\n")
  const metricsMap = {};
  // const metrics = {};
  // metrics.data =[];
  textByLine.forEach(function (line, index) {
    const lineEntries = line.split(",");
    if (index == 0) {
      //Skip the header
    } else {
      const fromDate = lineEntries[0];

      if (fromDate) {
        const toDate = lineEntries[1];
        const expUuid = lineEntries[2];
        const variantOneUuid = lineEntries[5];
        const variantTwoUuid = lineEntries[6];

        var metrics = metricsMap[[variantOneUuid, variantTwoUuid].join(',')];
        if (!metrics) {
          // console.log('Creating a new one ', [variantOneUuid, variantTwoUuid].join(','));

          metrics = {};
          metrics.experimentUuid = expUuid;
          metrics.fromDate = fromDate;
          metrics.toDate = toDate;
          metrics.measuredAt = new Date();
          metrics.variantOneUuid = variantOneUuid;
          metrics.variantTwoUuid = variantTwoUuid;
          metrics.data =[];

          metricsMap[[variantOneUuid, variantTwoUuid].join(',')] = metrics;
        } else {
          // console.log('Found an existing one ', [variantOneUuid, variantTwoUuid].join(','));
        }

        const metricEntry = {};
        metricEntry.metric = lineEntries[3];
        metricEntry.metricType = lineEntries[4];

        metricEntry.variantMetrics = [];
        var variantMetricEntry = {};
        variantMetricEntry.uuid = lineEntries[5]
        variantMetricEntry.value = lineEntries[7]
        metricEntry.variantMetrics.push(variantMetricEntry);

        variantMetricEntry = {};
        variantMetricEntry.uuid = lineEntries[6]
        variantMetricEntry.value = lineEntries[8]
        metricEntry.variantMetrics.push(variantMetricEntry);

        metricEntry.meanEffect = lineEntries[9];
        metricEntry.significance = lineEntries[10];
        metricEntry.meanEffectValue = lineEntries[11];
        // metricEntry.meanEffectValue = null;
        metricEntry.lowerBound = lineEntries[12];
        metricEntry.upperBound = lineEntries[13].trim();

        metrics.data.push(metricEntry);
      }
    }
  });
  return metricsMap;
}

const analysisCSVFileName = process.argv.slice(2)[0] || 'SampleMetrics.csv';
// This is an async file read
fs.readFile(analysisCSVFileName, 'utf-8', function (err, data) {
  if (err) {
    console.log("FATAL An error occurred trying to read in the file: " + err);
    process.exit(-2);
  }
  // Make sure there's data before we post it
  if(data) {
    const metricsMap = transformCsvToMetricsMap(data);

    const customerCounts = [];

    var analysisResultsCSVFileName = process.argv.slice(2)[1] || 'customerCounts.csv';
    fs.readFile(analysisResultsCSVFileName, 'utf-8', function (err, resultsData) {
      const customerCounts = getCustomerCounts(err, resultsData);

      Object.keys(metricsMap).forEach(function (key) {
        var metrics = metricsMap[key];
        metrics.customerCounts = customerCounts;

        const metricsJson = JSON.stringify(metrics);
        console.log(metricsJson)
        PostCode(metricsJson);
      });
    });
  }
  else {
    console.log("No data to post");
    process.exit(-1);
  }
});
