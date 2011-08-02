MSolr.SERVER = null;

/**
 * Sets the location as the default Solr Server.
 * 
 * @param {String} location The location of the server.
 */
MSolr.connect = function ( location ) {
  if ( location == null ) {
    location = "http://localhost:8983/solr";
  }

  MSolr.SERVER = location;
};

MSolr._checkConnection = function ( ) {
  if ( MSolr.SERVER == null ) {
    throw "Not connected to any Solr Server.";
  }
};

/*******************************************************************************
 * DB Plugin
 */

/**
 * Sets all the collection under this database for indexing.
 */
DB.prototype.solrIndex = function () {
  MSolr._checkConnection();
  MSolr.DEFAULT.index( MSolr.SERVER, this.getName() );
};

/**
 * Removes all the collection under this database from indexing.
 */
DB.prototype.dropSolrIndexes = function () {
  MSolr._checkConnection();
  MSolr.DEFAULT.server( MSolr.SERVER ).remove( this.getName() );
};

/**
 * Shows the indexing setting for this collection.
 */
DB.prototype.getSolrIndexes = function ( ) {
  var criteria = {};
  var dbRegexPattern = new RegExp( "^" + this.getName() + "\\..+" );

  MSolr._checkConnection();

  criteria[MSolrConst.SOLR_URL_KEY] = MSolr.SERVER;
  criteria[MSolrConst.NS_KEY] = dbRegexPattern;

  return MSolr.DEFAULT._getConfigColl().find( criteria );
};

(function () {
  var origHelpFunc = DB.prototype.help;
  DB.prototype.help = function () {
    origHelpFunc.apply( this );

    print("\tdb.solrIndex() - sets all collections under this db for Solr indexing");
    print("\tdb.getSolrIndexes() - shows the Solr indexing configuration for this db");
    print("\tdb.dropSolrIndex() - removes all collections under this db from Solr indexing");
  };
})();

/*******************************************************************************
 * Collection Plugin
 */

/**
 * Sets this collection for indexing to Solr.
 * 
 * @params {Array<String>} fields not yet supported
 */
DBCollection.prototype.solrIndex = function ( fields ) {
  MSolr._checkConnection();
  MSolr.DEFAULT.index( MSolr.SERVER, this.getFullName(), fields );
};

/**
 * Shows the indexing setting for this collection.
 */
DBCollection.prototype.getSolrIndexes = function ( ) {
  var namespace = this.getFullName();
  var criteria = {};

  MSolr._checkConnection();

  criteria[MSolrConst.SOLR_URL_KEY] = MSolr.SERVER;
  criteria[MSolrConst.NS_KEY] = namespace;

  return MSolr.DEFAULT._getConfigColl().find( criteria );
};

/**
 * Removes this collection from being indexed.
 */
DBCollection.prototype.dropSolrIndex = function ( fields ) {
  MSolr._checkConnection();
  MSolr.DEFAULT.server( MSolr.SERVER ).remove( this.getFullName(), fields );
};

/**
 * Removes this collection from being indexed.
 */
DBCollection.prototype.dropSolrIndexes = function ( ) {
  this.dropSolrIndex();
};

(function () {
  var origHelpFunc = DBCollection.prototype.help;
  DBCollection.prototype.help = function () {
    origHelpFunc.apply( this );

    var shortName = this.getName();
    print("\tdb." + shortName + ".solrIndex() - set this collection for Solr indexing");
    print("\tdb." + shortName + ".getSolrIndexes() - show the Solr indexing configuration for this collection");
    print("\tdb." + shortName + ".dropSolrIndex() - removes this collection from Solr indexing");
  };
})();

