/**
 * Simple helper class for testing javascript functions inside the Mongo Shell.
 */

var JSTester = {
  /**
   * Runs all the test of a given test object.
   * 
   * @param {Object} testClass The test object. Runs all methods with names ending in "Test".
   *   If the object has a setup or teardown methods, they will be executed for every test.
   */
  run: function ( testClass ) {
    var hasSetup = testClass.constructor.prototype.hasOwnProperty( "setup" );
    var hasTeardown = testClass.constructor.prototype.hasOwnProperty( "teardown" );

    for ( test in testClass ) {
      if ( /Test$/.test( test ) ) {
        if ( hasSetup ) {
          testClass.setup();
        }

        try {
          testClass[test]();
          JSTester.printResult( test, "OK", 70 );
        } catch ( e ) {
          print( e + "\n\n" );
          JSTester.printResult( test, "NG", 70 );
        }

        if ( hasTeardown ) {
          testClass.teardown();
        }
      }
    }
  },

  /**
   * Prints the result to stdout.
   * 
   * @param {String} testName The name for the test.
   * @param {String} result The result - OK, NG
   * @param {number} width The total number of characters to fill one line.
   */
  printResult: function ( testName, result, width ) {
    var extraChars = 4; //[  ]
    var dotCount = width - testName.length - result.length - extraChars;
    var dots = "";
    
    for ( var x = dotCount; x--; ) {
      dots += ".";
    }

    print( testName + dots + "[ " + result + " ]" );
  }
};

