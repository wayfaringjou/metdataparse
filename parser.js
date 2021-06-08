// Papa module for csv parsing and fs for file read/write
const Papa = require('papaparse');
const fs = require('fs');

// Setup file stream for reading
const file = fs.createReadStream('./MetObjects.csv');

// Globals. 'COUNT' keeps track of rows that pass conditions and 'OBJECTS' holds data entries
let OBJECTS = {};
let COUNT = 0;

// Urls for easy access
const apiEndpoint = 'https://collectionapi.metmuseum.org/public/collection/v1/objects/';
const metMuseumURL = 'https://www.metmuseum.org/art/collection/search/';

// Setup parser to connect to file stream using .pipe
const parse = Papa.parse(Papa.NODE_STREAM_INPUT, {header: true});
// Pipe the file input stream through the set up parser
file.pipe(parse)

/*
Columns:
-----------------
Object Number,
Is Highlight,
Is Timeline Work,
Is Public Domain,
Object ID,
Gallery Number,
Department,
AccessionYear,
Object Name,
Title,
Culture,
Period,
Dynasty,
Reign,
Portfolio,
Constituent ID,
Artist Role,
Artist Prefix,
Artist Display Name,
Artist Display Bio,
Artist Suffix,
Artist Alpha Sort,
Artist Nationality,
Artist Begin Date,
Artist End Date,
Artist Gender,
Artist ULAN URL,
Artist Wikidata URL,
Object Date,
Object Begin Date,
Object End Date,
Medium,
Dimensions,
Credit Line,
Geography Type,
City,
State,
County,
Country,
Region,
Subregion,
Locale,
Locus,
Excavation,
River,
Classification,
Rights and Reproduction,
Link Resource,
Object Wikidata URL,
Metadata Date,
Repository,
Tags,
Tags AAT URL,
Tags Wikidata URL
*/

// What to do for each row of data
const handleData = (row) => {
  // Set conditions for selecting rows
  if (row["Artist Display Name"]) {
    //console.log(row["Title"])
    //console.log(row)
    
    // Select rows to add to parsed document
    const filteredRow = {
      'Object ID': row['Object ID'],
      'Department': row['Department'],
      'Classification': row['Classification'],
      'Object Name': row['Object Name'],
      'Title': row['Title'],
      'Culture': row['Culture'],
      'Country': row['Country'],
      'Object Date': row['Object Date'],
      'Object Begin Date': row['Object Begin Date'],
      'Object End Date': row['Object End Date'],
      'Artist Display Name': row['Artist Display Name'],
      'Artist Begin Date': row['Artist Begin Date'],
      'Artist End Date': row['Artist End Date'],
      'Is Highlight': row['Is Highlight'],
      'API entry': `${apiEndpoint}${row['Object ID']}`,
      'Museum entry': `${metMuseumURL}${row['Object ID']}`,
    };
    
    // If department key doesn't exist, add it
    if (!(row['Department'] in OBJECTS)) {
      // Initialize with an empty array
      OBJECTS[row['Department']] = [];
      // Add current filtered row to array
      OBJECTS[row['Department']].push(filteredRow);
    } else if (row['Department'] in OBJECTS) {
      OBJECTS[row['Department']].push(filteredRow);
    }
    // Add to selected rows counter
    COUNT++;
  }
}

// What to do when the whole input file has been read
const handleFinish = () => {
  let deptCount = 0;

  Object.keys(OBJECTS).forEach((departmentKey) => {
    const parsedFile = fs.createWriteStream(`./ParsedMetObjects-${departmentKey}.csv`);
    const csvString = Papa.unparse(OBJECTS[departmentKey]);
    parsedFile.write(csvString);
    deptCount++;
  });
  console.log(`Distributed ${COUNT} rows to ${deptCount} document${deptCount > 1 ? 's': ''}.`);
}

// Use 'handleData' as callback for each row of data detected by parser pipe
parse.on('data', handleData);

// Use 'handleFinish' as callback when input file has been completely read
parse.on('end', handleFinish);
