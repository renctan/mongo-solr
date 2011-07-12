/**
 * Tests for msolr_db.js
 */

// Namespace wrapper
(function () {
var pathPrefix = "../../src/js/";

load(pathPrefix + "msolr_const.js");
load(pathPrefix + "msolr_db.js");
load(pathPrefix + "msolr.js");
load("../js_test_helper.js");

var CONFIG_DB_NAME = "MSolrDBTestConfigDB";
var CONFIG_COLL_NAME = "MSolrDBConfigColl";
var SOLR_SERVER_LOC = "http://mongo.solr.net/solr";
var TEST_DB_1_NAME = "MSolrDBTestDB_1";
var TEST_DB_1_COLL = ["ab", "cd", "ef", "gh"];

// Global Setup
(function () {
  var mongo = new Mongo();
  var db = mongo.getDB( TEST_DB_1_NAME );

  for( var x = TEST_DB_1_COLL.length; x--;  ) {
    db.createCollection( TEST_DB_1_COLL[x] );
  }
})();

(function () {
  var solrDB;
  var mongoConn_ = new Mongo();
  var configDB_ = mongoConn_.getDB( CONFIG_DB_NAME );
  var configColl_;

  var serverConfigCriteria_ = {};
  serverConfigCriteria_[MSolrConst.SOLR_URL_KEY] = SOLR_SERVER_LOC;

  var setup = function () {
    configColl_ = configDB_.getCollection( CONFIG_COLL_NAME );
    configColl_.insert( serverConfigCriteria_ );
    solrDB = new MSolrDb( configColl_, SOLR_SERVER_LOC, TEST_DB_1_NAME );
  };

  var teardown = function () {
    configDB_.dropDatabase();
  };

  /**
   * Runs the test with the proper hooks.
   * 
   * @param {Function} testFunc The test function to run.
   */
  var runTest = function ( testFunc ) {
    setup();
    MSolrJSTestHelper.test( testFunc );
    teardown();
  };

  var indexAllShouldIncludeAllCollectionTest = function () {
    var configDoc;
    var indexedColl;
    var indexResult;

    solrDB.indexAll();
    configDB_.getLastError();

    configDoc = configColl_.findOne( serverConfigCriteria_ );
    indexedColl = configDoc[MSolrConst.DB_LIST_KEY][TEST_DB_1_NAME];

    // TODO: Find a better way to check membership equality in js
    for( var x = TEST_DB_1_COLL.length; x--;  ) {
      assert.neq( undefined, indexedColl[TEST_DB_1_COLL[x]] );
    }
  };

  var indexShouldAddOneCollectionTest = function () {
    var configDoc;
    var indexResult;
    var newIndex = "qwerty";

    solrDB.index( newIndex );
    configDB_.getLastError();

    configDoc = configColl_.findOne( serverConfigCriteria_ );
    indexedColl = configDoc[MSolrConst.DB_LIST_KEY][TEST_DB_1_NAME];

    // TODO: Find a better way to check membership equality in js
    assert.neq( undefined, indexedColl[newIndex] );
  };

  var removeIndexTest = function () {
    var configDoc;
    var indexResult;
    var newIndex = "qwerty";

    solrDB.index( newIndex );
    solrDB.remove( newIndex );
    configDB_.getLastError();

    configDoc = configColl_.findOne( serverConfigCriteria_ );
    indexedColl = configDoc[MSolrConst.DB_LIST_KEY][TEST_DB_1_NAME];

    // TODO: Find a better way to check membership equality in js
    assert.eq( undefined, indexedColl[newIndex] );
  };

  runTest( indexAllShouldIncludeAllCollectionTest );
  runTest( indexShouldAddOneCollectionTest );
  runTest( removeIndexTest );
})();

// Global Teardown
(function () {
  var mongo = new Mongo();
  mongo.getDB( TEST_DB_1_NAME ).dropDatabase();
})();

})(); // Namespace wrapper

