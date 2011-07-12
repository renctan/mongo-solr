/**
 * Creates a helper class that updates the index configuration of a database.
 * 
 * @param {Collection} configColl The collection that contains the configuration info.
 * @param {String} serverLocation The location of the Solr server.
 * @param {String} dbName The name of the database.
 */
var MSolrDb = function ( configColl, serverLocation, dbName ){
  this.configColl = configColl;
  this.serverLocation = serverLocation;
  this.dbName = dbName;
  this.criteria = {};
  this.criteria[MSolrConst.SOLR_URL_KEY] = serverLocation;
  this.keyPrefix = MSolrConst.DB_LIST_KEY + "." + dbName + ".";
};

/**
 * Add all collections under this database for indexing.
 */
MSolrDb.prototype.indexAll = function (){
  var coll_names = this.getMongo().getDB(this.dbName).getCollectionNames();

  for (var x = coll_names.length; x--;) {
    add(coll_names[x]);
  }
};

/**
 * Add a collection to index.
 * 
 * @param {String} coll The name of the collection to index.
 * @param {String} [field = null] The name of the specific field to index. (Not yet supported)
 * 
 * Warning: Passing a null to the field will delete all previous field settings for that
 * collection.
 */
MSolrDb.prototype.index = function ( coll, field ){
  var elemKey = this.keyPrefix + coll;
  var docField = {};

  if ( field == null ) {
    docField[elemKey] = [];
    this.configColl.update( this.criteria, { $set: docField } );
  }
  else {
    docField[elemKey] = field;
    this.configColl.update( this.criteria, { $addToSet: docField } );
  }
};

/**
 * Removes a collection from being indexed.
 * 
 * @param {String} coll The name of the collection to remove.
 */
MSolrDb.prototype.remove = function ( coll ){
  var elemKey = this.keyPrefix + coll;
  var docField = {};

  docField[elemKey] = 1;
  this.configColl.update( this.criteria, { $unset: docField } );
};

