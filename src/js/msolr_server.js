/**
 * Creates a simple class that represents a Solr Server configuration.
 * 
 * @param {DBCollection} configColl The collection that contains the configuration info.
 * @param {String} loc The location of the Solr Server.
 * @param {Object} opt An optional object for specifying extra parameters. Recognized keys:
 * 
 *   {integer} repCount The number of servers to wait for a write operation to get
 *      replicated to.
 *   {integer} timeout The number of milliseconds to wait before repCount is satisfied.
 * 
 */
var MSolrServer = function ( configColl, loc, opt ) {
  opt = opt || {};

  this.repCount = opt.repCount || 1;
  this.timeout = opt.timeout || 1000;

  this.configColl = configColl;
  this.configDB = configColl.getDB();
  this.loc = loc;
};

/**
 * Sets the target for indexing.
 * 
 * @param name {String} Name of the database or namespace to index to Solr.
 * @param fields Not yet supported
 *
 * Ex:
 * server.index( "test" ); // Sets all collections under the test DB for indexing.
 * server.index( "test.user"); // Sets the user collection for indexing.
 */
MSolrServer.prototype.index = function ( name, fields ) {
  if ( name.indexOf(".") == -1 ) {
    this._indexByDB( name );
  }
  else {
    this._indexByNS( name, fields );
  }
};

/**
 * Removes the target from indexing.
 * 
 * @param name {String} Name of the database or namespace to index to Solr.
 * @param fields Not yet supported
 * 
 * Ex:
 * server.remove( "test" ); // Removes all collections under the test DB from indexing.
 * server.remove( "test.user"); // Removes the user collection from indexing.
 */
MSolrServer.prototype.remove = function ( name, fields ) {
  if ( name.indexOf(".") == -1 ) {
    this._removeByDB( name );
  }
  else {
    this._removeByNS( name, fields );
  }
};

/**
 * Add all collections under this database for indexing.
 * 
 * @param {String} dbName The name of the database.
 */
MSolrServer.prototype._indexByDB = function ( dbName ){
  var collNames = this.configColl.getMongo().getDB(dbName).getCollectionNames();
  var coll;

  for ( var x = collNames.length; x--; ) {
    coll = collNames[x];

    if ( !/^system\..*/.test( coll ) ) {
      this._indexByNS( dbName + "." + collNames[x], null );
    }
  }

  MSolrUtil.getLastError( this.configDB, this.repCount, this.timeout );
};

/**
 * Removes a database from indexing.
 * 
 * @param {String} dbName The name of the database.
 * @param {Boolean} [wait = false] Wait till the operation completes before returning.
 */
MSolrServer.prototype._removeByDB = function ( dbName, wait ) {
  var criteria = {};
  var doWait = wait || false;
  var dbRegexPattern = new RegExp( "^" + dbName + "\\..+" );

  criteria[MSolrConst.SOLR_URL_KEY] = this.loc;
  criteria[MSolrConst.NS_KEY] = dbRegexPattern;
  this.configColl.remove( criteria );
  MSolrUtil.getLastError( this.configDB, this.repCount, this.timeout );
};

/**
 * Add a collection to index. Assumes that there exist a document for the configuration of
 * the Solr server.
 * 
 * @param {String} ns The namespace of the collection to index.
 * @param {String} [field = null] The name of the specific field to index. (Not yet supported)
 */
MSolrServer.prototype._indexByNS = function ( ns, field ){
  var updateDoc = {};
  var fieldDoc = {};
  var criteria = {};

  criteria[MSolrConst.SOLR_URL_KEY] = this.loc;
  criteria[MSolrConst.NS_KEY] = ns;

  if ( field != null ) {
    fieldDoc[field] = MSolrDb.INDEX_COLL_FIELD_OPT;
    updateDoc[MSolrConst.COLL_FIELD_KEY] = fieldDoc;
  }

  this.configColl.update( criteria, { $set: updateDoc }, true );
  MSolrUtil.getLastError( this.configDB, this.repCount, this.timeout );
};

/**
 * Removes a collection from being indexed.
 * 
 * @param {String} ns The namespace of the collection to remove.
 */
MSolrServer.prototype._removeByNS = function ( ns, wait ){
  var criteria = {};

  criteria[MSolrConst.SOLR_URL_KEY] = this.loc;
  criteria[MSolrConst.NS_KEY] = ns;

  this.configColl.remove( criteria );
  MSolrUtil.getLastError( this.configDB, this.repCount, this.timeout );
};

MSolrServer.prototype.toString = function ( ) {
  return this.loc;
};

/**
 * Used for echo in the shell.
 */
MSolrServer.prototype.tojson = function ( ) {
  return this.toString();
};

