// Papa module for csv parsing and fs for file read/write
const Papa = require('papaparse');
const fs = require('fs');
const ProgressBar = require('progress');

// Setup file stream for reading
const file = fs.createReadStream('./MetObjects.csv');

// Stop if file not found
file.on('error', () => console.error("ERROR: Missing MetObjects.csv. Get it from https://github.com/metmuseum/openaccess.git"))

// Globals. 'COUNT' keeps track of rows that pass conditions and 'OBJECTS' holds data entries, 'DEPTID' museum's id for dept
let OBJECTS = {};
let COUNT = 0;
const DEPTID = {
  "The American Wing": 1,
  "Ancient Near Eastern Art": 3,
  "Arms and Armor": 4,
  "Arts of Africa, Oceania, and the Americas": 5,
  "Asian Art": 6,
  "The Cloisters": 7,
  "Costume Institute": 8,
  "Drawings and Prints": 9,
  "Egyptian Art": 10,
  "European Paintings": 11,
  "European Sculpture and Decorative Arts": 12,
  "Greek and Roman Art": 13,
  "Islamic Art": 14,
  "Robert Lehman Collection": 15,
  "The Libraries": 16,
  "Medieval Art": 17,
  "Musical Instruments": 18,
  "Photographs": 19,
  "Modern and Contemporary Art": 21,
}

// Urls for easy access
const apiEndpoint = 'https://collectionapi.metmuseum.org/public/collection/v1/objects/';
const metMuseumURL = 'https://www.metmuseum.org/art/collection/search/';

// Setup parser to connect to file stream using .pipe
const parse = Papa.parse(Papa.NODE_STREAM_INPUT, { header: true });
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

// Set progress bar
const bar = new ProgressBar(':bar :percent', { total: 273786, width: 50 });

// What to do for each row of data
const handleData = (row) => {
  // Set conditions for selecting rows
  if (row["Artist Display Name"]
    && !row["Artist Display Name"].toLowerCase().includes('painter')
    && !row["Artist Display Name"].toLowerCase().includes('anonymous')
    && !row["Artist Display Name"].toLowerCase().includes('unknown')) {
    //console.log(row["Title"])
    //console.log(row)

    // Select rows to add to parsed document
    const filteredRow = row['Object ID'];

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
    bar.tick();
  }
}



// What to do when the whole input file has been read
const handleFinish = () => {
  let deptCount = 0;

  const parsedFile = fs.createWriteStream(`./seed.collections.sql`);
  const collectionEntries = [];

  Object.keys(OBJECTS).forEach((departmentKey) => {
    deptCount++;
    // console.log(DEPTID);
    console.log(departmentKey);
    const id = DEPTID[departmentKey];
    console.log(id);
    const entryString = `(${id}, '${departmentKey}', ARRAY [${OBJECTS[departmentKey]}])`;
    collectionEntries.push(entryString);
    // console.log(OBJECTS[departmentKey]);
  });

  parsedFile.write(`TRUNCATE collections;\nINSERT INTO collections (id, name, objects) VALUES\n${collectionEntries.join(',')};`);

  console.log(`\nDistributed ${COUNT} rows to ${deptCount} collection${deptCount > 1 ? 's' : ''}.`);
}

// Use 'handleData' as callback for each row of data detected by parser pipe
parse.on('data', handleData);

// Use 'handleFinish' as callback when input file has been completely read
parse.on('end', handleFinish);
