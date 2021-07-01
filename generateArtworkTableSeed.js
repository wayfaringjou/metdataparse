// Papa module for csv parsing and fs for file read/write
const Papa = require("papaparse");
const fs = require("fs");
const ProgressBar = require("progress");

// Setup file stream for reading
const file = fs.createReadStream("./MetObjects.csv");

// Stop if file not found
file.on("error", () =>
  console.error(
    "ERROR: Missing MetObjects.csv. Get it from https://github.com/metmuseum/openaccess.git"
  )
);

// Globals. 'COUNT' keeps track of rows that pass conditions and 'OBJECTS' holds data entries, 'DEPTID' museum's id for dept

let COUNT = 0;
const TABLECOLUMNS = [
  '"Object ID"',
  '"Department"',
  '"Classification"',
  '"Object Name"',
  '"Title"',
  '"Culture"',
  '"Country"',
  '"Object Date"',
  '"Object Begin Date"',
  '"Object End Date"',
  '"Artist Display Name"',
  '"Artist Begin Date"',
  '"Artist End Date"',
  '"Is Highlight"',
  '"API entry"',
  '"Museum entry"',
];

const TABLEROWS = [];

// Urls for easy access
const apiEndpoint =
  "https://collectionapi.metmuseum.org/public/collection/v1/objects/";
const metMuseumURL = "https://www.metmuseum.org/art/collection/search/";

// Setup parser to connect to file stream using .pipe
const parse = Papa.parse(Papa.NODE_STREAM_INPUT, { header: true });
// Pipe the file input stream through the set up parser
file.pipe(parse);

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
const bar = new ProgressBar(":bar :percent", { total: 257141, width: 50 });

// What to do for each row of data
const handleData = (row) => {
  // Set conditions for selecting rows
  if (
    row["Artist Display Name"] &&
    !row["Artist Display Name"].toLowerCase().includes("painter") &&
    !row["Artist Display Name"].toLowerCase().includes("anonymous") &&
    !row["Artist Display Name"].toLowerCase().includes("unknown")
  ) {
    //console.log(row["Title"])
    //console.log(row)

    // Select rows to add to parsed document
    const filteredRow = [
      row["Object ID"],
      row["Department"] ? `'${row["Department"].replace(/\'/g,"''")}'` : "null",
      row["Classification"] ? `'${row["Classification"].replace(/\'/g,"''")}'` : "null",
      row["Object Name"] ? `'${row["Object Name"].replace(/\'/g,"''")}'` : "null",
      row["Title"] ? `'${row["Title"].replace(/\'/g,"''")}'` : "null",
      row["Culture"] ? `'${row["Culture"].replace(/\'/g,"''")}'` : "null",
      row["Country"] ? `'${row["Country"].replace(/\'/g,"''")}'` : "null",
      row["Object Date"] ? `'${row["Object Date"].replace(/\'/g,"''")}'` : "null",
      row["Object Begin Date"] ? `'${row["Object Begin Date"].replace(/\'/g,"''")}'` : "null",
      row["Object End Date"] ? `'${row["Object End Date"].replace(/\'/g,"''")}'` : "null",
      row["Artist Display Name"] ? `'${row["Artist Display Name"].replace(/\'/g,"''")}'` : "null",
      row["Artist Begin Date"] ? `'${row["Artist Begin Date"].replace(/\'/g,"''")}'` : "null",
      row["Artist End Date"] ? `'${row["Artist End Date"].replace(/\'/g,"''")}'` : "null",
      row["Is Highlight"].toLowerCase(),
      `'${apiEndpoint}${row["Object ID"]}'`,
      `'${metMuseumURL}${row["Object ID"]}'`,
    ];

    TABLEROWS.push(`(${filteredRow.join(",")})`);

    // Add to selected rows counter
    COUNT++;
    bar.tick();
  }
};

// What to do when the whole input file has been read
const handleFinish = () => {
  let deptCount = 0;

  const parsedFile = fs.createWriteStream(`./seed.artwork.sql`);
  // const collectionEntries = [];

  // Object.keys(OBJECTS).forEach((departmentKey) => {
  //   deptCount++;
  //   // console.log(DEPTID);
  //   console.log(departmentKey);
  //   const id = DEPTID[departmentKey];
  //   console.log(id);
  //   const entryString = `(${id}, '${departmentKey}', ARRAY [${OBJECTS[departmentKey]}])`;
  //   collectionEntries.push(entryString);
  //   // console.log(OBJECTS[departmentKey]);
  // });

  parsedFile.write(
    `TRUNCATE artworks;\nINSERT INTO artworks (${TABLECOLUMNS.join(
      ","
    )}) VALUES\n${TABLEROWS.join(",\n")};`
  );

  console.log(`\nAdded ${COUNT} rows to 'seed.artwork.sql'.`);
};

// Use 'handleData' as callback for each row of data detected by parser pipe
parse.on("data", handleData);

// Use 'handleFinish' as callback when input file has been completely read
parse.on("end", handleFinish);
