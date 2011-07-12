/**
 * Creates a simple class that represents a Solr Server configuration.
 * 
 * @param {Collection} configColl The collection that contains the configuration info.
 * @param {String} loc The location of the Solr Server.
 */
var MSolrServer = function ( configColl, loc ) {
  this.configColl = configColl;
  this.loc = loc;

  this.criteria = {};
  this.criteria[MSolrConst.SOLR_URL_KEY] = loc;
};

/**
 * Gets a database configuration for this server.
 * 
 * @param {String} dbName The name of the database to get.
 */
MSolrServer.prototype.db = function ( dbName ) {
  return new MSolrDb( this.configColl, this.loc, dbName );
};

/**
 * Removes a database from indexing.
 * 
 * @param {String} dbName The name of the database.
 */
MSolrServer.prototype.removeDB = function ( dbName ) {
  var docField = {};
  docField[MSolrConst.DB_LIST_KEY + "." + dbName] = 1;

  this.configColl.update( this.criteria, { $unset: docField } );
};

