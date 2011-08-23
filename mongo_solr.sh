#! /bin/sh

# A very simple script to start the Mongo shell preloaded with the plugin.
# This script assumes that the Mongo shell is in the executable path.

echo "Loading MongoSolr extensions..."
mongo $* --eval "load('solr-plugin.js')" --shell

