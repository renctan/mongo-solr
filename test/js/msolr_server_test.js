/**
 * Tests for msolr_server.js
 */

// Namespace wrapper
(function () {
var pathPrefix = "../../src/js/";

load(pathPrefix + "msolr_const.js");
load(pathPrefix + "msolr_db.js");
load(pathPrefix + "msolr_server.js");
load(pathPrefix + "msolr.js");
load("../js_test_helper.js");

var CONFIG_DB_NAME = "MSolrServerTestConfigDB";
var CONFIG_COLL_NAME = "MSolrDBConfigColl";
var SOLR_SERVER_LOC = "http://mongo.solr.net/solr";
var TEST_DB_1_NAME = "MSolrServerTestDB_1";
var TEST_DB_2_NAME = "MSolrServerTestDB_2";

(function () {
  var solr;
  var mongoConn_ = new Mongo();
  var configDB_ = mongoConn_.getDB( CONFIG_DB_NAME );
  var configColl_;

  var serverConfigCriteria_ = {};
  serverConfigCriteria_[MSolrConst.SOLR_URL_KEY] = SOLR_SERVER_LOC;

  var setup = function () {
    configColl_ = configDB_.getCollection( CONFIG_COLL_NAME );
    configColl_.insert( serverConfigCriteria_ );
    solr = new MSolrServer( configColl_, SOLR_SERVER_LOC );
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

  var dbTest = function () {
    var configDoc;
    var indexedDB;
    var indexResult;
    var solrDB = solr.db( TEST_DB_1_NAME );

    // Needs to create a collection in order to have the database created.
    solrDB.index( "dummy" );
    configDB_.getLastError();

    configDoc = configColl_.findOne( serverConfigCriteria_ );
    indexedDB = configDoc[MSolrConst.DB_LIST_KEY];

    // TODO: Find a better way to check membership equality in js
    assert.neq( undefined, indexedDB[TEST_DB_1_NAME] );
  };

  var removeDBwithOneDBExistingTest = function () {
    var configDoc;
    var indexedDB;
    var indexResult;
    var solrDB = solr.db( TEST_DB_1_NAME );

    // Needs to create a collection in order to have the database created.
    solrDB.index( "dummy" );
    solr.removeDB( TEST_DB_1_NAME );
    configDB_.getLastError();

    configDoc = configColl_.findOne( serverConfigCriteria_ );
    indexedDB = configDoc[MSolrConst.DB_LIST_KEY];

    // TODO: Find a better way to check membership equality in js
    assert.eq( undefined, indexedDB[TEST_DB_1_NAME] );
  };

  var removeDBwithTwoDBExistingTest = function () {
    var configDoc;
    var indexedDB;
    var indexResult;

    // Needs to create a collection in order to have the database created.
    solr.db( TEST_DB_1_NAME ).index( "dummy" );
    solr.db( TEST_DB_2_NAME ).index( "another_dummy" );

    solr.removeDB( TEST_DB_1_NAME );
    configDB_.getLastError();

    configDoc = configColl_.findOne( serverConfigCriteria_ );
    indexedDB = configDoc[MSolrConst.DB_LIST_KEY];

    // TODO: Find a better way to check membership equality in js
    assert.eq( undefined, indexedDB[TEST_DB_1_NAME] );
    assert.neq( undefined, indexedDB[TEST_DB_2_NAME] );
  };

  runTest( dbTest );
  runTest( removeDBwithOneDBExistingTest );
  runTest( removeDBwithTwoDBExistingTest );
})();

})(); // Namespace wrapper

