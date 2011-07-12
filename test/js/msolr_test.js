/**
 * Tests for msolr.js
 */

// Namespace wrapper
(function () {
var pathPrefix = "../../src/js/";

load(pathPrefix + "msolr_const.js");
load(pathPrefix + "msolr_server.js");
load(pathPrefix + "msolr.js");
load("../js_test_helper.js");

var CONFIG_DB_NAME = "MSolrTestConfigDB";
var CONFIG_COLL_NAME = "MSolrConfigColl";
var SOLR_SERVER1_LOC = "http://mongo.solr.net/solr";
var SOLR_SERVER2_LOC = "http://another.solr.server/solr";

(function () {
  var msolr;
  var mongoConn_ = new Mongo();
  var configDB_ = mongoConn_.getDB( CONFIG_DB_NAME );
  var configColl_;

  var setup = function () {
    configDB_.createCollection( CONFIG_COLL_NAME );
    configColl_ = configDB_.getCollection( CONFIG_COLL_NAME );
    msolr = new MSolr( CONFIG_DB_NAME, CONFIG_COLL_NAME );
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

  var getConfigIfAvailableTest = function () {
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

  var getLocalIfConfigNotFoundTest = function () {
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

  var addServerTest = function () {
    // TODO: implement
  };

  var changeUrlTest = function () {
    // TODO: implement
  };

  var removeServerTest = function () {
    var serverConfigCriteria_ = {};
    var doc;

    msolr.addServer( SOLR_SERVER1_LOC );
    msolr.removeServer( SOLR_SERVER1_LOC );
    configDB_.getLastError();

    serverConfigCriteria_[MSolrConst.SOLR_URL_KEY] = SOLR_SERVER1_LOC;
    doc = configColl_.findOne( serverConfigCriteria_ );

    assert.eq( null, doc );
  };

  var removeServerWithTwoServersTest = function () {
    // TODO: implement
  };

  runTest( getConfigIfAvailableTest );
  runTest( getLocalIfConfigNotFoundTest );
  runTest( addServerTest );
  runTest( changeUrlTest );
  runTest( removeServerTest );
  runTest( removeServerWithTwoServersTest );
})();

})(); // Namespace wrapper

