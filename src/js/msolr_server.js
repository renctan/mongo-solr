/**
 * Creates a simple class that represents a Solr Server configuration.
 * 
 * @param configColl [Collection] The collection that contains the configuration info.
 * @param loc [String] The location of the Solr Server.
 */
var MSolrServer = function( configColl, loc ) {
  this.configColl = configColl;
  this.loc = loc;

  this.criteria = {};
  this.criteria[MSolrConst.SOLR_URL_KEY] = loc;
};

/**
 * Gets a database configuration for this server.
 * 
 * @param dbName [String] The name of the database to add
 * @param doIncludeAllCollection [Boolean] (true) Whether to index all the collections
 *   under the new database.
 */
MSolrServer.prototype.db = function( dbName ) {
  return new MSolrDb( this.configColl, this.loc, dbName );
};

/**
 * Removes a database from indexing.
 * 
 * @param dbName [String] The name of the database.
 */
MSolrServer.prototype.removeDB = function( dbName ) {
  this.configColl.update( this.criteria, { $unset: {dbName: 1} } );
};

