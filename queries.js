const Papa = require('papaparse');
const fs = require('fs');
// const { log } = require('./methods');
const file = fs.createReadStream('./MetObjects.csv');
let ARTISTS = {}

const parse = Papa.parse(Papa.NODE_STREAM_INPUT, {
  header: true,
  preview: 2,
	/*
  step: function(row, parser) {
    console.log('///')
		console.log(row.data["Artist Display Name"]);
    ARTISTS[row.data["Object ID"]] = row.data["Artist Display Name"];
	},
  complete: function (results, file) {
    console.log(ARTISTS);
    return;
  },
  error: function() {
    console.log('error')
  }*/
});
// console.log(parse)
const handleData = (d) => console.log(d)
file.pipe(parse)
parse.on('data', handleData)
parse.on('end', (e) => console.log('Finish'))
// return ARTISTS;