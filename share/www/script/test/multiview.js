couchTests.multiview = function(debug) {
    var db = new CouchDB("test_suite_db", {"X-Couch-Full-Commit":"false"});
    var multiQueryUrl = "/test_suite_db/_multi";

    db.deleteDb();
    db.createDb();
    if (debug) debugger;
    
    var docs = makeDocs(100);
    
    // create the docs
    var results = db.bulkSave(docs);

    T(results.length == 100);
    
    // create two views, one that emits when doc.integer is a multiple of 3 and another that emits when a multiple of 4
    // we will then do a multiview that intersects the views above to give the a view that contains only doc.integer values which are multiples of 12 (3 and 4)
    var ddocThree = {
      _id:"_design/three",
      views: {
        "test" : {map: "function (doc) {if (doc.integer % 3 == 0) emit(doc.integer, null)};"}
      }
    };

    var ddocFour = {
      _id:"_design/four",
      views: {
        "test" : {map: "function (doc) {if (doc.integer % 4 == 0) emit(doc.integer, null)};"}
      }
    };
    
    T(db.save(ddocThree).ok);
    T(db.save(ddocFour).ok);
    
    // create the multiview post doc
    var multiview_req = CouchDB.request("POST", multiQueryUrl, {body: JSON.stringify({"views" : ["test_suite_db/_design/three/_view/test", "test_suite_db/_design/four/_view/test"]})});
    results = JSON.parse(multiview_req.responseText);

    T(results.rows);
    
    for (row in results.rows)
      T(parseInt(results.rows[row]) % 12 == 0);
      
    // test a single view, this takes its own route
    multiview_req =  CouchDB.request("POST", multiQueryUrl, {body: JSON.stringify({"views" : ["test_suite_db/_design/three/_view/test"]})});
    results = JSON.parse(multiview_req.responseText);

    T(results.rows);   

    for (row in results.rows)
      T(parseInt(results.rows[row]) % 3 == 0); 
}
