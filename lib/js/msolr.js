/**
 * A plugin for the MongoDB Javascript shell client that can configure the settings for
 * the mongo-solr daemon.
 */

/**
* Creates a simple class that is connected to the mongo-solr configuration server.
*
* @param {string} configDBName Optional parameter for specifying the name of the
* configuration database for mongo-solr
* @param {string} configCollName Optional parameter for specifying the name of the
* configuration collection for mongo-solr
*/
var MSolr = function ( configDBName, configCollName ){
  var conn = db.getMongo();
  var dbName = configDBName || MSolr.getConfigDBName( conn );
  var collName = configCollName || MSolrConst.CONFIG_COLLECTION_NAME;
  var ensureIdxCriteria = {};
  var rsStat = rs.status();
  var members = 0;
  var memberList;

  this.db = conn.getDB( dbName );
  this.coll = this.db.getCollection( collName );
  ensureIdxCriteria[MSolrConst.SOLR_URL_KEY] = 1;
  ensureIdxCriteria[MSolrConst.NS_KEY] = 1;

  if ( rsStat.errmsg == null ) {
    memberList = rsStat.members;

    for ( var i = memberList.length; i--; ) {
      if ( memberList[i].state == 1 || memberList[i].state == 2 ) {
        members++;
      }
    }

    this.repCount = members;
  }
  else {
    this.repCount = 1;
  }

  this.coll.ensureIndex( ensureIdxCriteria, { "unique": true } );
};

MSolr._wtimeout = 2000; // 2 sec

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

  // Always use config since local db is not replicated to secondaries
/*
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
*/
  return ret;
};

/**
 * Gets the settings of all indexing servers.
 * 
 * @return {DBQuery} The cursor to the configuration documents.
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
 */
MSolr.prototype.changeUrl = function ( originalUrl, newUrl ) {
  var criteria = {};
  var docField = {};

  criteria[MSolrConst.SOLR_URL_KEY] = originalUrl;
  docField[MSolrConst.SOLR_URL_KEY] = newUrl;
  this.coll.update( criteria, { $set: docField } );

  MSolrUtil.getLastError( this.db, this.repCount, MSolr._wtimeout );
};

/**
 * Deletes the server configuration.
 * 
 * @param {String} location The location of the server.
 */
MSolr.prototype.removeServer = function ( location ) {
  var criteria = {};

  criteria[MSolrConst.SOLR_URL_KEY] = location;
  this.coll.remove( criteria );

  MSolrUtil.getLastError( this.db, this.repCount, MSolr._wtimeout );
};

/**
 * Get the server configuration object.
 * 
 * @param {String} location The location of the server.
 * 
 * @return {MSolrServer} the server object.
 */
MSolr.prototype.server = function ( location ) {
  var opt = {
    repCount: this.repCount,
    timeout: MSolr._wtimeout
  };

  return new MSolrServer( this.coll, location, opt );
};

/**
 * Sets the indexing configuration.
 * 
 * @param {String} server The location of the Solr server to index to.
 * @param {String} db The name of the database to index.
 * @param {String} [coll = null] The name of the collection to index. All collections under
 *   db will be indexed if this is not specified.
 * @param fields {Object} The names of the fields of this object are the only fields that
 *   will be included when indexing a document. The values of this object are currently
 *   ignored. All of the fields are indexed if this object is empty or null.
 */
MSolr.prototype.index = function ( server, db, coll, fields ) {
  if ( coll == null ) {
    this.server( server ).index( db );
  }
  else {
    this.server( server ).index( db + "." + coll, fields );
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

/**
 * Deletes all the index configuration data.
 */
MSolr.prototype.reset = function ( ) {
  return this.coll.remove();
};

///////////////////////////////////////////////////////
// Static methods

MSolr.DEFAULT = new MSolr();

/**
 * @see MSolr#showConfig
 */
MSolr.showConfig = function ( ) {
  return MSolr.DEFAULT.showConfig();
};

/**
 * @see MSolr#reset
 */
MSolr.reset = function ( ) {
  return MSolr.DEFAULT.reset();
};

