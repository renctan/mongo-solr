/**
 * Tests for msolr_db.js
 */

// Namespace wrapper
(function () {
var pathPrefix = "../../src/js/";

load(pathPrefix + "msolr_const.js");
load(pathPrefix + "msolr_db.js");
load(pathPrefix + "msolr.js");
load("../jstester.js");

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

var MSolrDBTest = function () {
  this.solrDB = null;
  this.mongoConn = new Mongo();
  this.configDB = this.mongoConn.getDB( CONFIG_DB_NAME );
  this.configColl = null;

  this.serverConfigCriteria = {};
  this.serverConfigCriteria[MSolrConst.SOLR_URL_KEY] = SOLR_SERVER_LOC;
};

MSolrDBTest.prototype.setup = function () {
  this.configColl = this.configDB.getCollection( CONFIG_COLL_NAME );
  this.configColl.insert( this.serverConfigCriteria );
  this.solrDB = new MSolrDb( this.configColl, SOLR_SERVER_LOC, TEST_DB_1_NAME );
};

MSolrDBTest.prototype.teardown = function () {
  this.configDB.dropDatabase();
};

MSolrDBTest.prototype.indexAllShouldIncludeAllCollectionTest = function () {
  var configDoc;
  var indexedColl;
  var indexResult;

  this.solrDB.indexAll();
  this.configDB.getLastError();

  configDoc = this.configColl.findOne( this.serverConfigCriteria );
  indexedColl = configDoc[MSolrConst.DB_LIST_KEY][TEST_DB_1_NAME];

  // TODO: Find a better way to check membership equality in js
  for( var x = TEST_DB_1_COLL.length; x--;  ) {
    assert.neq( undefined, indexedColl[TEST_DB_1_COLL[x]] );
  }
};

MSolrDBTest.prototype.indexShouldAddOneCollectionTest = function () {
  var configDoc;
  var indexResult;
  var newIndex = "qwerty";

  this.solrDB.index( newIndex );
  this.configDB.getLastError();

  configDoc = this.configColl.findOne( this.serverConfigCriteria );
  indexedColl = configDoc[MSolrConst.DB_LIST_KEY][TEST_DB_1_NAME];

  // TODO: Find a better way to check membership equality in js
  assert.neq( undefined, indexedColl[newIndex] );
};

MSolrDBTest.prototype.removeIndexTest = function () {
  var configDoc;
  var indexResult;
  var newIndex = "qwerty";
  var indexedColl;

  this.solrDB.index( newIndex );
  this.solrDB.remove( newIndex );
  this.configDB.getLastError();

  configDoc = this.configColl.findOne( this.serverConfigCriteria );
  indexedColl = configDoc[MSolrConst.DB_LIST_KEY][TEST_DB_1_NAME];

  // TODO: Find a better way to check membership equality in js
  assert.eq( undefined, indexedColl[newIndex] );
};

JSTester.run( new MSolrDBTest() );

// Global Teardown
(function () {
  var mongo = new Mongo();
  mongo.getDB( TEST_DB_1_NAME ).dropDatabase();
})();

})(); // Namespace wrapper

