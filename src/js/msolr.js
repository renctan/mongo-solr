/**
 * A plugin for the MongoDB Javascript shell client that can configure the settings for
 * the mongo-solr daemon.
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
  var collName = configCollName || MSolrConst.CONFIG_COLLECTION_NAME;
  var ensureIdxCriteria = {};

  this.db = conn.getDB( dbName );
  this.coll = this.db.getCollection( collName );
  ensureIdxCriteria[MSolrConst.SOLR_URL_KEY] = 1;
  ensureIdxCriteria[MSolrConst.NS_KEY] = 1;

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
MSolr.prototype.showConfig = function () {
  return this.coll.find();
};

/**
 * Change the url of a server to a new url. Please note that this operation is not
 * atomic as there can be several config documents matching the url.
 * 
 * @param {String} originalUrl The original url.
 * @param {String} newUrl The new url.
 * @param {Boolean} [wait = false] Wait till the operation completes before returning.
 */
MSolr.prototype.changeUrl = function ( originalUrl, newUrl, wait ) {
  var criteria = {};
  var docField = {};
  var doWait = wait || false;

  criteria[MSolrConst.SOLR_URL_KEY] = originalUrl;
  docField[MSolrConst.SOLR_URL_KEY] = newUrl;
  this.coll.update( criteria, { $set: docField } );

  if ( doWait ) {
    this.db.getLastError();
  }
};

/**
 * Deletes the server configuration.
 * 
 * @param {String} location The location of the server.
 * @param {Boolean} [wait = false] Wait till the operation completes before returning.
 */
MSolr.prototype.removeServer = function ( location, wait ) {
  var criteria = {};
  var doWait = wait || false;

  criteria[MSolrConst.SOLR_URL_KEY] = location;
  this.coll.remove( criteria );

  if ( doWait ) {
    this.db.getLastError();
  }
};

/**
 * Get the server configuration object.
 * 
 * @param {String} location The location of the server.
 * 
 * @return {MSolrServer} the server object.
 */
MSolr.prototype.server = function ( location ) {
  return new MSolrServer( this.coll, location );
};

/**
 * Sets the indexing configuration.
 * 
 * @param {String} server The location of the Solr server to index to.
 * @param {String} db The name of the database to index.
 * @param {String} [coll = null] The name of the collection to index. All collections under
 *   db will be indexed if this is not specified.
 * @param {String} [field = null] The name of the field to index (not yet supported)
 */
MSolr.prototype.index = function ( server, db, coll, field ) {
  if ( coll == null ) {
    this.server( server ).index( db );
  }
  else {
    this.server( server ).index( db + "." + coll, field );
  }
};

MSolr.prototype.toString = function ( ) {
  return "config_collection: " + this.coll.getFullName();
};

/**
 * Used for echo in the shell.
 */
MSolr.prototype.tojson = function ( ) {
  return this.toString();
};

/**
 * Gets the indexing configuration collection.
 */
MSolr.prototype._getConfigColl = function ( ) {
  return this.coll;
};

