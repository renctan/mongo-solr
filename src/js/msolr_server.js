/**
 * Creates a simple class that represents a Solr Server configuration.
 * 
 * @param {Collection} configColl The collection that contains the configuration info.
 * @param {String} loc The location of the Solr Server.
 */
var MSolrServer = function ( configColl, loc ) {
  var insertDoc = {};

  this.configColl = configColl;
  this.configDB = configColl.getDB();
  this.loc = loc;

  insertDoc[MSolrConst.SOLR_URL_KEY] = this.loc;
  this.configColl.update( insertDoc, { $set: insertDoc }, true );
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
 * @param {Boolean} [wait = false] Wait till the operation completes before returning.
 */
MSolrServer.prototype.removeDB = function ( dbName, wait ) {
  var criteria = {};
  var pullCriteria = {};
  var pullInnerCrit = {};
  var doWait = wait || false;
  var dbRegexPattern = new RegExp( "^" + dbName + "\\..+" );

  criteria[MSolrConst.SOLR_URL_KEY] = this.loc;
  pullInnerCrit[MSolrConst.NS_KEY] = dbRegexPattern;
  pullCriteria[MSolrConst.LIST_KEY] = pullInnerCrit;

  this.configColl.update( criteria, { $pull: pullCriteria } );

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

