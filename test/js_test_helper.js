/**
 * Simple helper class for testing javascript functions inside the Mongo Shell.
 */

var MSolrJSTestHelper = {
  test: function ( testFunc ) {
    try {
      testFunc();
    } catch ( e ) {
      print( e + "\n\n" );
    }
  }
};

