# Overview

A simple Ruby script for indexing the entire contents of a MongoDB instance (excluding administrative collections) to Solr. Since the scripts relies on polling the oplogs to synchronize the contents of the database with Solr, the database needs to be running on master/slave or replica set configuration.

# Features

 - Ability to specify passwords to authenticate through databases.

# TODO
 - Add a fancier API for using the script

# External Gem Dependencies:

## For running the script:
  gem install rsolr mongo

## For running the tests:
  gem install shoulda mocha

# Running the test:

The tests are located at the test/unit and test/integration respectively.

## Integration Test Assumptions

The integration test uses the following assumptions:

1. The database server is running locally and using port 27107.
2. The database server is running on a master slave configuration.
3. There is no admin user registered on the database.
4. The database server is running with security mode on (by passing the --auth option).
5. The test sets the output of the logger to "/dev/null" so the system running it should be able
   to understand it.

