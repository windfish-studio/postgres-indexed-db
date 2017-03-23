Postgres - IndexedDB Import/Export
=========

Use this library to export a Postgres database to JSON and then import it to HTML5 
IndexedDB. Makes use of streams to handle I/O for databases larger than available RAM.

## Usage

Note, the package will export either the exporter or the importer depending on
the environment. E.g. if you're in a browser, require('postgres-indexed-db') will return the
importer, as well as assign it to window.PGIndexedDBImporter. 

Both importer and exporter will return RxJS Observable objects, which will emit useful progress
messages.


### Export (Node.js Only)
```js
    var exporter = require('postgres-indexed-db');
    
    //exporter config.db is fed into pg.Pool https://www.npmjs.com/package/pg
    var observable = exporter({
      db: {
          database: "dvdrental_sample"
      },
      output_path: "test/exported_data",
      results_per_page: 5000
    });
                      
    observable.subscribe(function(msg_o){
        switch(msg_o.message){
            case "success":
                console.log('finished');
                //Data is output to JSON in the output_path;
                //one .json file per db table.
                break;
            case "progress":
                console.log("%"+(msg_o.value*100).toFixed(2)+" done...");
                break;
        }
    });
```

### Import (Browser Only)
```js
    //Import 
    var importer = require('postgres-indexed-db');
    
    //path to data from exporter as 1st param, IndexedDB name as 2nd
    var observable = importer( "test/exported_data", 'test_db' );
    
    observable.subscribe(function(msg_o){
        switch (msg_o.message){
            case "success":
                console.log('finished');
                //Schema + data has been imported to IndexedDB. Indices from Postgres have
                //also been migrated.
                break;
            case "progress":
                console.log((msg_o.value * 100).toFixed(2) + '% done...');
                break;
            case "manifest":
                manifest = msg_o.value;
                break;
        }
    });
    
```

## Running The Tests

To run the tests, first create a postgres database called 'dvdrental_sample' and 
extract test/test_db.zip and use the resulting .sql file to populate the database
with test data. 

Run test/test.js to test the exporter, and then open 
test/test.html in a browser to test the importer.