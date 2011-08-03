var MSolrUtil = {
  /**
   * Performs a getLastError command and prints out a message if an error occured.
   * 
   * @param {DB} db The database object on which to execute the command
   * @param {integer} w The w parameter for the getLastError command
   * @param {integer} wtimeout The wtimeout parameter for the getLastError command
   */
  getLastError: function ( db, w, wtimeout ) {
    var result = db.runCommand( { getlasterror: 1, w: w, wtimeout: wtimeout } );

    if ( result.err != null ) {
      print( "Failed to apply changes to all replicas: " + result.err );
    }
  }
};

