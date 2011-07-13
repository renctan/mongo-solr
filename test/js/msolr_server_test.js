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
load("../jstester.js");

var CONFIG_DB_NAME = "MSolrServerTestConfigDB";
var CONFIG_COLL_NAME = "MSolrDBConfigColl";
var SOLR_SERVER_LOC = "http://mongo.solr.net/solr";
var TEST_DB_1_NAME = "MSolrServerTestDB_1";
var TEST_DB_2_NAME = "MSolrServerTestDB_2";

var MSolrServerTest = function () {
  this.solr = null;
  this.mongoConn = new Mongo();
  this.configDB = this.mongoConn.getDB( CONFIG_DB_NAME );
  this.configColl = null;

  this.serverConfigCriteria = {};
  this.serverConfigCriteria[MSolrConst.SOLR_URL_KEY] = SOLR_SERVER_LOC;
};

MSolrServerTest.prototype.setup = function () {
  this.configColl = this.configDB.getCollection( CONFIG_COLL_NAME );
  this.configColl.insert( this.serverConfigCriteria );
  this.solr = new MSolrServer( this.configColl, SOLR_SERVER_LOC );
};

MSolrServerTest.prototype.teardown = function () {
  this.configDB.dropDatabase();
};

MSolrServerTest.prototype.dbTest = function () {
  var configDoc;
  var indexedDB;
  var indexResult;
  var solrDB = this.solr.db( TEST_DB_1_NAME );

  // Needs to create a collection in order to have the database created.
  solrDB.index( "dummy" );
  this.configDB.getLastError();

  configDoc = this.configColl.findOne( this.serverConfigCriteria );
  indexedDB = configDoc[MSolrConst.DB_LIST_KEY];

  // TODO: Find a better way to check membership equality in js
  assert.neq( undefined, indexedDB[TEST_DB_1_NAME] );
};

MSolrServerTest.prototype.removeDBwithOneDBExistingTest = function () {
  var configDoc;
  var indexedDB;
  var indexResult;
  var solrDB = this.solr.db( TEST_DB_1_NAME );

  // Needs to create a collection in order to have the database created.
  solrDB.index( "dummy" );
  this.solr.removeDB( TEST_DB_1_NAME );
  this.configDB.getLastError();

  configDoc = this.configColl.findOne( this.serverConfigCriteria );
  indexedDB = configDoc[MSolrConst.DB_LIST_KEY];

  // TODO: Find a better way to check membership equality in js
  assert.eq( undefined, indexedDB[TEST_DB_1_NAME] );
};

MSolrServerTest.prototype.removeDBwithTwoDBExistingTest = function () {
  var configDoc;
  var indexedDB;
  var indexResult;

  // Needs to create a collection in order to have the database created.
  this.solr.db( TEST_DB_1_NAME ).index( "dummy" );
  this.solr.db( TEST_DB_2_NAME ).index( "another_dummy" );

  this.solr.removeDB( TEST_DB_1_NAME );
  this.configDB.getLastError();

  configDoc = this.configColl.findOne( this.serverConfigCriteria );
  indexedDB = configDoc[MSolrConst.DB_LIST_KEY];

  // TODO: Find a better way to check membership equality in js
  assert.eq( undefined, indexedDB[TEST_DB_1_NAME] );
  assert.neq( undefined, indexedDB[TEST_DB_2_NAME] );
};

JSTester.run( new MSolrServerTest() );

})(); // Namespace wrapper

