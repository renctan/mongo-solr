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

var TEST_DB_1 = {
  name: "MSolrDBTestDB_1",
  coll: ["ab", "cd", "ef", "gh"]
};

var TEST_DB_2 = {
  name: "MSolrDBTestDB_2",
  coll: ["system.index", "ab"]
};

var TEST_DB_3 = {
  name: "MSolrDBTestDB_3",
  coll: ["deeply.nested.coll"]
};

/**
 * Extracts the name of the collection from the namespace name.
 * 
 * @namespace {String} namespace The namespace string that includes the name of the
 *   database and collection.
 * 
 * @return {String} the name of the collection.
 */
var extractCollName = function ( namespace ) {
  var split = namespace.split( "." );
  split.shift();
  return split.join(".");
};

/**
 * Gets the index of the first object to be found inside an array.
 * 
 * @param {Array} array The array to test.
 * @param {Object} memberToTest The object to find.
 * 
 * @return {number} the index of the member if found and -1 if not.
 */
var arrayFind = function ( array, memberToTest ) {
  var index = -1;

  for ( var x = array.length; x--; ) {
    if ( memberToTest == array[x] ) {
      index = x;
      break;
    }
  }

  return index;
};

// Global Setup
(function () {
  var mongo = new Mongo();
  var db;
  var coll;
  var x;

  db = mongo.getDB( TEST_DB_1.name );
  coll = TEST_DB_1.coll;
  for( x = coll.length; x--; ) {
    db.createCollection( coll[x] );
  }

  db = mongo.getDB( TEST_DB_2.name );
  coll = TEST_DB_2.coll;
  for( x = coll.length; x--; ) {
    db.createCollection( coll[x] );
  }

  db = mongo.getDB( TEST_DB_3.name );
  coll = TEST_DB_3.coll;
  for( x = coll.length; x--; ) {
    db.createCollection( coll[x] );
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
  this.solrDB = new MSolrDb( this.configColl, SOLR_SERVER_LOC, TEST_DB_1.name );
};

MSolrDBTest.prototype.teardown = function () {
  this.configDB.dropDatabase();
};

MSolrDBTest.prototype.indexAllShouldIncludeAllCollectionTest = function () {
  var configCur;
  var resultCount = 0;
  var testColl = TEST_DB_1.coll;

  this.solrDB.indexAll( true );

  configCur = this.configColl.find( this.serverConfigCriteria );
  configCur.forEach( function ( doc ) {
    var collName = extractCollName( doc[MSolrConst.NS_KEY] );
    resultCount += 1;
    assert.neq( -1, arrayFind( testColl, collName ) );
  });

  assert.eq( testColl.length, resultCount );
};

MSolrDBTest.prototype.indexAllShouldNotIncludeSystemCollectionTest = function () {
  var configDoc;
  var indexedColl;
  var indexResult;
  var solr = new MSolrDb( this.configColl, SOLR_SERVER_LOC, TEST_DB_2.name );

  solr.indexAll( true );

  configDoc = this.configColl.findOne( this.serverConfigCriteria );
  indexedColl = configDoc[MSolrConst.NS_KEY];

  for( var ns in indexedColl ) {
    assert( !/^[^.]*\.system\..*/.test( ns ) );
  }
};

MSolrDBTest.prototype.indexAllShouldProperlySetDottedCollectionNamesTest = function () {
  var configDoc;
  var indexedColl;
  var indexResult;
  var solr = new MSolrDb( this.configColl, SOLR_SERVER_LOC, TEST_DB_3.name );
  var ns = TEST_DB_3.name + "." + TEST_DB_3.coll[0];

  solr.indexAll( true );

  configDoc = this.configColl.findOne( this.serverConfigCriteria );
  assert.eq( ns, configDoc[MSolrConst.NS_KEY] );
};

MSolrDBTest.prototype.indexShouldAddOneCollectionTest = function () {
  var configDoc;
  var newIndex = "qwerty";
  var ns = TEST_DB_1.name + "." + newIndex;

  this.solrDB.index( newIndex, null, true );
  configDoc = this.configColl.findOne( this.serverConfigCriteria );

  assert.eq( ns, configDoc[MSolrConst.NS_KEY] );
};

MSolrDBTest.prototype.removeIndexTest = function () {
  var configDoc;
  var indexResult;
  var newIndex = "qwerty";
  var ns = TEST_DB_1.name + "." + newIndex;

  this.solrDB.index( newIndex );
  this.solrDB.remove( newIndex, true );
  configDoc = this.configColl.findOne( this.serverConfigCriteria );

  assert.eq( null, configDoc );
};

JSTester.run( new MSolrDBTest() );

// Global Teardown
(function () {
  var mongo = new Mongo();
  mongo.getDB( TEST_DB_1.name ).dropDatabase();
  mongo.getDB( TEST_DB_2.name ).dropDatabase();
  mongo.getDB( TEST_DB_3.name ).dropDatabase();
})();

})(); // Namespace wrapper

