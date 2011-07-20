/**
 * Creates a helper class that updates the index configuration of a database.
 * 
 * @param {Collection} configColl The collection that contains the configuration info.
 * @param {String} serverLocation The location of the Solr server.
 * @param {String} dbName The name of the database.
 */
var MSolrDb = function ( configColl, serverLocation, dbName ){
  this.configColl = configColl;
  this.configDB = configColl.getDB();
  this.serverLocation = serverLocation;
  this.dbName = dbName;
};

/**
 * Add all collections under this database for indexing.
 * 
 * @param {Boolean} [wait = false] Wait till the operation completes before returning.
 */
MSolrDb.prototype.indexAll = function ( wait ){
  var collNames = this.configColl.getMongo().getDB(this.dbName).getCollectionNames();
  var doWait = wait || false;
  var coll;

  for ( var x = collNames.length; x--; ) {
    coll = collNames[x];

    if ( !/^system\..*/.test( coll ) ) {
      this.index( collNames[x], null );
    }
  }

  if ( doWait ) {
    this.configDB.getLastError();
  }
};

/**
 * Add a collection to index. Assumes that there exist a document for the configuration of
 * the Solr server.
 * 
 * @param {String} coll The name of the collection to index.
 * @param {String} [field = null] The name of the specific field to index. (Not yet supported)
 * @param {Boolean} [wait = false] Wait till the operation completes before returning.
 */
MSolrDb.prototype.index = function ( coll, field, wait ){
  var ns = this.dbName + "." + coll;
  var doWait = wait || false;

  var updateDoc = {};
  var fieldDoc = {};
  var newDoc = {};
  var neUpdateCriteria = {};
  var updateCriteria = {};
  var nsCritKey = MSolrConst.LIST_KEY + "." + MSolrConst.NS_KEY;
  var updateKey = MSolrConst.LIST_KEY + ".$." + MSolrConst.NS_KEY;

  var updateResult;

  updateCriteria[MSolrConst.SOLR_URL_KEY] = this.serverLocation;
  neUpdateCriteria["$ne"] = ns;
  updateCriteria[nsCritKey] = neUpdateCriteria;

  newDoc[MSolrConst.NS_KEY] = ns;
  // TODO: add fields
  updateDoc[MSolrConst.LIST_KEY] = newDoc;

  this.configColl.update( updateCriteria, { $push: updateDoc } );

  if ( field != null ) {
    updateResult = this.configDB.runCommand( { $getLastError: 1 } );

    if ( updateResult.n == 0 ) {
      // There is an existing entry for the collection already, so modify it directly
      updateCriteria = {};
      updateCriteria[MSolrConst.SOLR_URL_KEY] = this.serverLocation;
      updateCriteria[nsCritKey] = ns;

      // TODO: add code for setting fields
    }
  }

  if ( doWait ) {
    this.configDB.getLastError();
  }
};

/**
 * Removes a collection from being indexed.
 * 
 * @param {String} coll The name of the collection to remove.
 * @param {Boolean} [wait = false] Wait till the operation completes before returning.
 */
MSolrDb.prototype.remove = function ( coll, wait ){
  var ns = this.dbName + "." + coll;
  var criteria = {};
  var doWait = wait || false;
  var updateDoc = {};
  var pullCriteria = {};

  criteria[MSolrConst.SOLR_URL_KEY] = this.serverLocation;
  pullCriteria[MSolrConst.NS_KEY] = ns;
  updateDoc[MSolrConst.LIST_KEY] = pullCriteria;

  this.configColl.update( criteria, { $pull: updateDoc } );

  if ( doWait ) {
    this.configDB.getLastError();
  }
};

MSolrDb.prototype.toString = function ( ) {
  return this.dbName + " -> " + this.serverLocation;
};

/**
 * Used for echo in the shell.
 */
MSolrDb.prototype.tojson = function ( ) {
  return this.toString();
};

