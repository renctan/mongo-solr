/**
 * Tests for msolr_server.js
 */

// Namespace wrapper
(function () {
var pathPrefix = "../../src/js/";

load(pathPrefix + "msolr_const.js");
load(pathPrefix + "util.js");
load(pathPrefix + "msolr_server.js");
load(pathPrefix + "msolr.js");
load("../jstester.js");

var CONFIG_DB_NAME = "MSolrServerTestConfigDB";
var CONFIG_COLL_NAME = "MSolrDBConfigColl";
var SOLR_SERVER_LOC = "http://mongo.solr.net/solr";

var TEST_DB_1 = {
  name: "MSolrServerTestDB_1",
  coll: ["ab", "cd", "ef", "gh"]
};

var TEST_DB_2 = {
  name: "MSolrServerTestDB_2",
  coll: ["system.index", "ab"]
};

var TEST_DB_3 = {
  name: "MSolrServerTestDB_3",
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

MSolrServerTest.prototype.removeDBwithOneDBExistingTest = function () {
  var criteria = {};

  this.solr.index( TEST_DB_1.name, null );
  this.solr.remove( TEST_DB_1.name, null );

  assert.eq( 0, this.configColl.count() );
};

MSolrServerTest.prototype.removeDBwithTwoDBExistingTest = function () {
  this.solr.index( TEST_DB_1.name );
  // Index a dummy collection
  this.solr.index( TEST_DB_2.name + ".dummy", null );

  this.solr.remove( TEST_DB_1.name, null );

  this.configColl.find( this.serverConfigCriteria ).forEach( function ( doc ) {
    // Only expecting one result
    assert.eq( TEST_DB_2.name + ".dummy", doc[MSolrConst.NS_KEY] );
  });
};

MSolrServerTest.prototype.indexUsingDbNameShouldIncludeAllCollectionTest = function () {
  var resultCount = 0;
  var testColl = TEST_DB_1.coll;

  this.solr.index( TEST_DB_1.name, null );

  this.configColl.find( this.serverConfigCriteria ).forEach( function ( doc ) {
    var collName = extractCollName( doc[MSolrConst.NS_KEY] );
    assert.neq( -1, arrayFind( testColl, collName ) );
    resultCount++;
  });

  assert.eq( testColl.length, resultCount );
};

MSolrServerTest.prototype.indexUsingDbNameShouldNotIncludeSystemCollectionTest = function () {
  var testColl = TEST_DB_2.coll;
  var resultCount = 0;

  this.solr.index( TEST_DB_2.name, null );

  this.configColl.find( this.serverConfigCriteria ).forEach( function ( doc ) {
    var collName = extractCollName( doc[MSolrConst.NS_KEY] );
    assert( !/^[^.]*\.system\..*/.test( collName ) );
    assert.neq( -1, arrayFind( testColl, collName ) );
    resultCount++;
  });

  assert.eq( 1, resultCount );
};

MSolrServerTest.prototype.indexUsingDbNameShouldProperlySetDottedCollectionNamesTest = function () {
  var ns = TEST_DB_3.name + "." + TEST_DB_3.coll[0];

  this.solr.index( TEST_DB_3.name, null );

  this.configColl.find( this.serverConfigCriteria ).forEach( function ( doc ) {
    // Expecting only a single result
    assert.eq( ns, doc[MSolrConst.NS_KEY] );
  });
};

MSolrServerTest.prototype.indexShouldAddOneCollectionTest = function () {
  var ns = TEST_DB_1.name + ".qwerty";

  this.solr.index( ns, null );
  this.configColl.find( this.serverConfigCriteria ).forEach( function ( doc ) {
    // Expecting only a single result
    assert.eq( ns, doc[MSolrConst.NS_KEY] );
  });
};

MSolrServerTest.prototype.removeIndexTest = function () {
  var criteria = {};
  var ns = TEST_DB_1.name + ".qwerty";

  this.solr.index( ns, null );
  this.solr.remove( ns, null );

  criteria[MSolrConst.SOLR_SERVER_LOC] = SOLR_SERVER_LOC;
  criteria[MSolrConst.NS_KEY] = ns;

  assert.eq( 0, this.configColl.count( criteria ) );
};

JSTester.run( new MSolrServerTest() );

// Global Teardown
(function () {
  var mongo = new Mongo();
  mongo.getDB( TEST_DB_1.name ).dropDatabase();
  mongo.getDB( TEST_DB_2.name ).dropDatabase();
  mongo.getDB( TEST_DB_3.name ).dropDatabase();
})();

})(); // Namespace wrapper

