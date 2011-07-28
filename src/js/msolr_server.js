/**
 * Creates a simple class that represents a Solr Server configuration.
 * 
 * @param {Collection} configColl The collection that contains the configuration info.
 * @param {String} loc The location of the Solr Server.
 */
var MSolrServer = function ( configColl, loc ) {
  this.configColl = configColl;
  this.configDB = configColl.getDB();
  this.loc = loc;
};

/**
 * Sets the target for indexing.
 * 
 * @param name {String} Name of the database or namespace to index to Solr.
 * @param fields Not yet supported
 * @param {Boolean} [wait = false] Wait till the operation completes before returning.
 * 
 * Ex:
 * server.index( "test" ); // Sets all collections under the test DB for indexing.
 * server.index( "test.user"); // Sets the user collection for indexing.
 */
MSolrServer.prototype.index = function ( name, fields, wait ) {
  if ( name.indexOf(".") == -1 ) {
    this._indexByDB( name, wait );
  }
  else {
    this._indexByNS( name, fields, wait );
  }
};

/**
 * Removes the target from indexing.
 * 
 * @param name {String} Name of the database or namespace to index to Solr.
 * @param fields Not yet supported
 * @param {Boolean} [wait = false] Wait till the operation completes before returning.
 * 
 * Ex:
 * server.remove( "test" ); // Removes all collections under the test DB from indexing.
 * server.remove( "test.user"); // Removes the user collection from indexing.
 */
MSolrServer.prototype.remove = function ( name, fields, wait ) {
  if ( name.indexOf(".") == -1 ) {
    this._removeByDB( name, wait );
  }
  else {
    this._removeByNS( name, fields, wait );
  }
};

/**
 * Add all collections under this database for indexing.
 * 
 * @param {String} dbName The name of the database.
 * @param {Boolean} [wait = false] Wait till the operation completes before returning.
 */
MSolrServer.prototype._indexByDB = function ( dbName, wait ){
  var collNames = this.configColl.getMongo().getDB(dbName).getCollectionNames();
  var doWait = wait || false;
  var coll;

  for ( var x = collNames.length; x--; ) {
    coll = collNames[x];

    if ( !/^system\..*/.test( coll ) ) {
      this._indexByNS( dbName + "." + collNames[x], null );
    }
  }

  if ( doWait ) {
    this.configDB.getLastError();
  }
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

  if ( doWait ) {
    this.configDB.getLastError();
  }
};

/**
 * Add a collection to index. Assumes that there exist a document for the configuration of
 * the Solr server.
 * 
 * @param {String} ns The namespace of the collection to index.
 * @param {String} [field = null] The name of the specific field to index. (Not yet supported)
 * @param {Boolean} [wait = false] Wait till the operation completes before returning.
 */
MSolrServer.prototype._indexByNS = function ( ns, field, wait ){
  var doWait = wait || false;
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

  if ( doWait ) {
    this.configDB.getLastError();
  }
};

/**
 * Removes a collection from being indexed.
 * 
 * @param {String} ns The namespace of the collection to remove.
 * @param {Boolean} [wait = false] Wait till the operation completes before returning.
 */
MSolrServer.prototype._removeByNS = function ( ns, wait ){
  var criteria = {};
  var doWait = wait || false;

  criteria[MSolrConst.SOLR_URL_KEY] = this.loc;
  criteria[MSolrConst.NS_KEY] = ns;

  this.configColl.remove( criteria );

  if ( doWait ) {
    this.configDB.getLastError();
  }
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

