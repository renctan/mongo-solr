/**
 * Tests for msolr.js
 */

// Namespace wrapper
(function () {
var pathPrefix = "../../src/js/";

load(pathPrefix + "msolr_const.js");
load(pathPrefix + "msolr_db.js");
load(pathPrefix + "msolr_server.js");
load(pathPrefix + "msolr.js");
load("../jstester.js");

var CONFIG_DB_NAME = "MSolrTestConfigDB";
var CONFIG_COLL_NAME = "MSolrConfigColl";
var SOLR_SERVER1_LOC = "http://mongo.solr.net/solr";
var SOLR_SERVER2_LOC = "http://another.solr.server/solr";

var MSolrTest = function () {
  this.mongoConn = new Mongo();
  this.configDB = this.mongoConn.getDB( CONFIG_DB_NAME );
  this.configColl = null;
  this.msolr = null;
};

MSolrTest.prototype.setup = function () {
  this.configDB = this.mongoConn.getDB( CONFIG_DB_NAME );
  this.configDB.createCollection( CONFIG_COLL_NAME );
  this.configColl = this.configDB.getCollection( CONFIG_COLL_NAME );
  this.msolr = new MSolr( CONFIG_DB_NAME, CONFIG_COLL_NAME );
};

MSolrTest.prototype.teardown = function () {
  this.configDB.dropDatabase();
};

MSolrTest.prototype.getConfigIfAvailableTest = function () {
  var mockMongo = {
    getDBs: function () {
      return {
        databases: [
          {
            "name" : "test",
            "sizeOnDisk" : 67108864,
            "empty" : false
          },
          {
            "name" : "admin",
            "sizeOnDisk" : 67108864,
            "empty" : false
          },
          {
            "name" : "config",
            "sizeOnDisk" : 67108864,
            "empty" : false
          },
          {
            "name" : "local",
            "sizeOnDisk" : 134217728,
            "empty" : false
          }
        ]
      };
    }
  };

  assert.eq( "config", MSolr.getConfigDBName( mockMongo ));
};

MSolrTest.prototype.getLocalIfConfigNotFoundTest = function () {
  var mockMongo = {
    getDBs: function () {
      return {
        databases: [
          {
            "name" : "test",
            "sizeOnDisk" : 67108864,
            "empty" : false
          },
          {
            "name" : "admin",
            "sizeOnDisk" : 67108864,
            "empty" : false
          }
        ]
      };
    }
  };

  assert.eq( "local", MSolr.getConfigDBName( mockMongo ) );
};

MSolrTest.prototype.basicChangeUrlTest = function () {
  var doc;

  // Needs to index something before a server config entry can appear
  this.msolr.server( SOLR_SERVER1_LOC ).db( "dummy" ).index( "coll" );
  this.msolr.changeUrl( SOLR_SERVER1_LOC, SOLR_SERVER2_LOC, true );

  doc = this.configColl.findOne();
  assert.eq( SOLR_SERVER2_LOC, doc[MSolrConst.SOLR_URL_KEY] );
};

MSolrTest.prototype.changeUrlWithNonExistingOriginalUrlTest = function () {
  var doc;
  var cursor;

  // Needs to index something before a server config entry can appear
  this.msolr.server( SOLR_SERVER1_LOC ).db( "dummy" ).index( "coll" );
  this.msolr.changeUrl( SOLR_SERVER2_LOC, "Hilfe", true );

  cursor = this.configColl.find();
  cursor.forEach( function ( doc ) {
    // There should be only one result
    assert.eq( SOLR_SERVER1_LOC, doc[MSolrConst.SOLR_URL_KEY] );
  });
};

MSolrTest.prototype.removeServerTest = function () {
  var serverConfigCriteria = {};
  var doc;

  // Needs to index something before a server config entry can appear
  this.msolr.server( SOLR_SERVER1_LOC ).db( "dummy" ).index( "coll" );
  this.msolr.removeServer( SOLR_SERVER1_LOC, true );

  serverConfigCriteria[MSolrConst.SOLR_URL_KEY] = SOLR_SERVER1_LOC;
  doc = this.configColl.findOne( serverConfigCriteria );

  assert.eq( null, doc );
};

MSolrTest.prototype.removeServerWithTwoServersTest = function () {
  var cursor;

  this.msolr.server( SOLR_SERVER1_LOC ).db( "dummy" ).index( "coll" );
  this.msolr.server( SOLR_SERVER2_LOC ).db( "dummy2" ).index( "coll" );
  this.msolr.removeServer( SOLR_SERVER1_LOC, true );

  cursor = this.configColl.find();
  cursor.forEach( function ( doc ) { // There should be only one result
    assert.eq( SOLR_SERVER2_LOC, doc[MSolrConst.SOLR_URL_KEY] );
  });
};

JSTester.run( new MSolrTest() );

})(); // Namespace wrapper

