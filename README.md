# Overview

A simple Ruby script for indexing the contents of a MongoDB instance to Solr. Since the scripts relies on polling the oplogs to synchronize the contents of the database with Solr, the database needs to be running on master/slave or replica set configuration.

Please check out the wiki for more details about this project.

# Features

* Automatically retries failed operations to Mongo or Solr (The script assumes that the link to Mongo/Solr is just temporarily broken and can be resolved momentarily). 
* Periodically sets checkpoints and automatically resumes from them.
* Includes a client plugin for Mongo shell.
* Supports indexing to multiple Solr Servers.
* Supports connection to a replica set instance.
* Supports connection to a sharded cluster.

# Known issues

* There is an issue with the BSON extension binary that comes with the Ruby driver that will cause the daemon to run in unexpected ways. This is a machine dependent bug and to check if you're machine is susceptible to this issue, try executing this [snippet](https://gist.github.com/92eb07eebfe362a7f97c) in the Ruby interpreter. The output of BSON Ruby and C should be the same.

# TODO

* Provide a secured way of specifying username and password for authenticating the databases.

# Ruby version

The scripts can run on both v1.8.7 and 1.9.x, but it is not fully tested on v1.8.7, so it is recommended to use this with v1.9.

# Installation

Simply run the install task to install the dameon and the shell plugin client:

    sudo rake gem:install

# Usage

To run the daemon, simply call the executable after installing. For more details on the configurable options, run the script with the -h option:

    msolrd -h

# External Gem Dependencies:

Run the following command to install all the gem dependencies used by this project:

    bundle install

Note: You can get bundle from [here](http://gembundler.com/). And make sure that the gem binary is included in the default executable path.

# Running the test:

    rake test:all

## Test Assumptions

The integration test uses the following assumptions:

1. The database server is running locally and using port 27107.
2. The database server is running on a master slave or replica set configuration.
3. There is no admin user registered on the database. This is because the tests assumes that no authentication is needed to access the database.
4. The test sets the output of the logger to "/dev/null" so the system running it should be able
   to understand it.
5. There is no other process accessing the database server.

The slow tests needs a Solr Server running on the default http://localhost:8983/solr. However, it can delete the entire contents of the server so don't use a server with important data when running the tests.

## Note on running the tests

The tests uses the test-unit gem instead of one built-in to the MRI library. It also uses Mocha (v0.9.12), which unfortunately breaks the test-unit (v2.3.1) result reporting. The dots does not appear on successful test, but the E and F still appears. The one line summary still shows the correct results but will always display "0% passed" even if all the tests passed.

