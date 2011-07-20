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
  this.solr = new MSolrServer( this.configColl, SOLR_SERVER_LOC );
};

MSolrServerTest.prototype.teardown = function () {
  this.configDB.dropDatabase();
};

MSolrServerTest.prototype.creatingNewInstanceInsertsNewEntryTest = function () {
  var result = this.configColl.findOne( this.serverConfigCriteria );

  assert.neq( null, result );
};

MSolrServerTest.prototype.creatingAnotherNewInstanceDoesntInsertsNewEntryTest = function () {
  var anotherSolr = new MSolrServer( this.configColl, SOLR_SERVER_LOC );
  assert.eq( 1, this.configColl.count( this.serverConfigCriteria ) );
};

MSolrServerTest.prototype.dbTest = function () {
  var configDoc;
  var solrDB = this.solr.db( TEST_DB_1_NAME );

  // Needs to create a collection in order to have the database created.
  solrDB.index( "dummy", null, true );

  configDoc = this.configColl.findOne( this.serverConfigCriteria );
  assert.eq( TEST_DB_1_NAME + ".dummy", configDoc[MSolrConst.LIST_KEY][0][MSolrConst.NS_KEY] );
};

MSolrServerTest.prototype.removeDBwithOneDBExistingTest = function () {
  var configDoc;
  var solrDB = this.solr.db( TEST_DB_1_NAME );

  // Needs to create a collection in order to have the database created.
  solrDB.index( "dummy" );
  this.solr.removeDB( TEST_DB_1_NAME, true );

  configDoc = this.configColl.findOne( this.serverConfigCriteria );
  assert.eq( 0, configDoc[MSolrConst.LIST_KEY].length );
};

MSolrServerTest.prototype.removeDBwithTwoDBExistingTest = function () {
  var result;
  var nsSet;

  // Needs to create a collection in order to have the database created.
  this.solr.db( TEST_DB_1_NAME ).index( "dummy" );
  this.solr.db( TEST_DB_2_NAME ).index( "another_dummy" );

  this.solr.removeDB( TEST_DB_1_NAME, true );

  result = this.configColl.findOne( this.serverConfigCriteria );
  nsSet = result[MSolrConst.LIST_KEY];

  for ( var x = nsSet.length; x--; ) {
    // Only expecting one result
    assert.eq( TEST_DB_2_NAME + ".another_dummy", nsSet[x][MSolrConst.NS_KEY] );
  }
};

JSTester.run( new MSolrServerTest() );

})(); // Namespace wrapper

