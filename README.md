# Overview

A simple Ruby script for indexing the entire contents of a MongoDB instance (excluding administrative collections) to Solr. Since the scripts relies on polling the oplogs to synchronize the contents of the database with Solr, the database needs to be running on master/slave or replica set configuration.

Please check out the wiki for more details about this project.

# Ruby version

The scripts can run on both v1.8.7 and 1.9.x, but it is not fully tested on v1.8.7, so it is recommended to use this with v1.9.

# Usage

Simply run the mongo_solr.rb on the root directory. For more details on the configurable options, run the script with the -h option:

    ruby mongo_solr.rb -h

# External Gem Dependencies:

Run the following command to install all the gem dependencies used by this project:

    bundle install

Note: You can get bundle from [here](http://gembundler.com/). And make sure that the gem binary is included in the default executable path.

# Running the test:

    rake test:all

## Integration and JS Test Assumptions

The integration test uses the following assumptions:

1. The database server is running locally and using port 27107.
2. The database server is running on a master slave or replica set configuration.
3. There is no admin user registered on the database.
4. The test sets the output of the logger to "/dev/null" so the system running it should be able
   to understand it.
5. There is no other process accessing the database server.

