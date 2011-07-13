/**
 * A plugin for the MongoDB Javascript shell client that can configure the settings for
 * the mongo-solr daemon.
 */

/* Uncomment for debugging
load("msolr_const.js");
load("msolr_server.js");
load("msolr_db.js");
*/

/**
 * Creates a simple class that is connected to the mongo-solr configuration server.
 * 
 * @param {string} configDBName Optional parameter for specifying the name of the
 *   configuration database for mongo-solr
 * @param {string} configCollName Optional parameter for specifying the name of the
 *   configuration collection for mongo-solr
 */
var MSolr = function ( configDBName, configCollName ){
  var conn = new Mongo();
  var dbName = configDBName || MSolr.getConfigDBName( conn );
  var collName = configCollName || MSolrConst.MONGO_SOLR_COLLECTION_NAME;
  var ensureIdxCriteria = {};

  this.coll = conn.getDB( dbName ).getCollection( collName );
  ensureIdxCriteria[MSolrConst.SOLR_URL_KEY] = 1;
  this.coll.ensureIndex( ensureIdxCriteria, { "unique": true } );
};

/**
 * Determines which database to use when storing the configuration information.
 *
 * @param {Mongo} mongo The connection to the database.
 *
 * @return {String} The name of the database to use.
 */
MSolr.getConfigDBName = function ( mongo ) {
  var dbList;
  var configDBFound = false;
  var ret = "config";

  dbList = mongo.getDBs().databases;
  for ( var x = dbList.length; x--; ) {
    if ( dbList[x].name == "config" ) {
      configDBFound = true;
      break;
    }
  }

  if ( ! configDBFound ) {
    ret = "local";
  }

  return ret;
};


/**
 * Gets the settings of all indexing servers.
 * 
 * @return {Array} The list of server settings.
 */
MSolr.prototype.listServers = function () {
  return this.coll.find();
};

/**
 * Adds a new Solr server configuration.
 * 
 * @param {String} location The location of the server.
 */
MSolr.prototype.addServer = function ( location ) {
  var criteria = {};
  criteria[MSolrConst.SOLR_URL_KEY] = location;
  // Assumption: SOLR_URL_KEY values are unique -> currently enforced by the constructor.
  this.coll.insert( criteria );
};

/**
 * Change the url of a server to a new url.
 * 
 * @param {String} originalUrl The original url.
 * @param {String} newUrl The new url.
 */
MSolr.prototype.changeUrl = function ( originalUrl, newUrl ) {
  var criteria = {};
  var docField = {};

  criteria[MSolrConst.SOLR_URL_KEY] = originalUrl;
  docField[MSolrConst.SOLR_URL_KEY] = newUrl;
  this.coll.update( criteria, { $set: docField } );
};

/**
 * Delete a indexing server configuration.
 * 
 * @param {String} location The location of the server.
 */
MSolr.prototype.removeServer = function ( location ) {
  var criteria = {};
  criteria[MSolrConst.SOLR_URL_KEY] = location;
  this.coll.remove( criteria );
};

/**
 * Get the server configuration object.
 * 
 * @param {String} location The location of the server.
 * 
 * @return {MSolrServer} the server object.
 */
MSolr.prototype.getServer = function ( location ) {
  return new MSolrServer( this.coll, location );
};

