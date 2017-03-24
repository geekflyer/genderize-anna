global.Promise = require('bluebird');
process.env.NODE_ENV = 'development';
const xlsx = require('node-xlsx');
const axios = require('axios');
const fs = require('fs');
const _ = require('lodash');
const ProgressBar = require('progress');
const apiKey = require('./apikey.json').key;

const workSheetsFromFile = xlsx.parse(`${__dirname}/in/20170322 Unique eMail addresses.xlsx`);

const firstWorkSheet = workSheetsFromFile[0];

const firstWorksheetData = firstWorkSheet.data;

const header = firstWorksheetData[0];

const FIRST_NAME = 'First Name';

const colIndexMapping = {
  [FIRST_NAME]: null,
  gender: null,
  probability: null,
  count: null
};

Object.keys(colIndexMapping).forEach(colName => {
  colIndexMapping[colName] = header.findIndex(headerCell => colName === headerCell);
});

console.log(`determined colindexMapping to be:`);
console.dir(colIndexMapping);

function getGenders(upTo10NamesArr) {
  const baseUrl = `https://api.genderize.io/?apikey=${apiKey}&`;

  const requestParams = upTo10NamesArr.map(name => `name[]=${encodeURIComponent(name)}`).join('&');

  const requestUrl = baseUrl + requestParams;

  console.log(`requesting from: ${requestUrl}`);
  return axios.get(baseUrl + requestParams)
    .then(response => response.data);
  // .tap(res => console.dir(res))
}

const rowData = firstWorksheetData.slice(1);

const rowDataChunksOfSizeMax10 = _.chunk(rowData, 10);

console.log(`there are ${rowDataChunksOfSizeMax10.length} chunks of size 10 to be processed`);

const bar = new ProgressBar(':bar :percent :current / :total :elapsed :eta', {total: rowData.length});

return Promise.map(rowDataChunksOfSizeMax10, rowChunk => {
  return getGenders(rowChunk.map(getFirstName))
    .then(chunkResults => {
      chunkResults.forEach((result, indexInChunk) => {
        rowChunk[indexInChunk][colIndexMapping.gender] = result.gender;
        rowChunk[indexInChunk][colIndexMapping.probability] = result.probability;
        rowChunk[indexInChunk][colIndexMapping.count] = result.count;
      });
      bar.tick(rowChunk.length);
      return rowChunk;
    });
}, {concurrency: 20})
  .then(_.flatten)
  .then(enrichedData => {

    // console.log(`first 2 records of enrichedData look like this:`);
    // console.dir(enrichedData.slice(0,2));

    const enrichedFile = xlsx.build([{
      name: firstWorkSheet.name, data: [header].concat(enrichedData)
    }]);

    fs.writeFileSync(`${__dirname}/out/out.xlsx`, enrichedFile);
  });


function getFirstName(row) { return row[colIndexMapping[FIRST_NAME]]}
