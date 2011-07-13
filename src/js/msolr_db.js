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
  this.criteria = {};
  this.criteria[MSolrConst.SOLR_URL_KEY] = serverLocation;
  this.keyPrefix = MSolrConst.DB_LIST_KEY + "." + dbName + ".";
};

/**
 * Add all collections under this database for indexing.
 * 
 * @param {Boolean} wait Wait till the operation completes before returning. false by default.
 */
MSolrDb.prototype.indexAll = function ( wait ){
  var coll_names = this.configColl.getMongo().getDB(this.dbName).getCollectionNames();
  var doWait = wait || false;

  for (var x = coll_names.length; x--;) {
    this.index( coll_names[x] );
  }

  if ( doWait ) {
    this.configDB.getLastError();
  }
};

/**
 * Add a collection to index.
 * 
 * @param {String} coll The name of the collection to index.
 * @param {String} [field = null] The name of the specific field to index. (Not yet supported)
 * @param {Boolean} wait Wait till the operation completes before returning. false by default.
 *  
 * Warning: Passing a null to the field will delete all previous field settings for that
 * collection.
 */
MSolrDb.prototype.index = function ( coll, field, wait ){
  var elemKey = this.keyPrefix + coll;
  var docField = {};
  var doWait = wait || false;

  if ( field == null ) {
    docField[elemKey] = [];
    this.configColl.update( this.criteria, { $set: docField } );
  }
  else {
    docField[elemKey] = field;
    this.configColl.update( this.criteria, { $addToSet: docField } );
  }

  if ( doWait ) {
    this.configDB.getLastError();
  }
};

/**
 * Removes a collection from being indexed.
 * 
 * @param {String} coll The name of the collection to remove.
 * @param {Boolean} wait Wait till the operation completes before returning. false by default.
 */
MSolrDb.prototype.remove = function ( coll, wait ){
  var elemKey = this.keyPrefix + coll;
  var docField = {};
  var doWait = wait || false;

  docField[elemKey] = 1;
  this.configColl.update( this.criteria, { $unset: docField } );

  if ( doWait ) {
    this.configDB.getLastError();
  }
};

