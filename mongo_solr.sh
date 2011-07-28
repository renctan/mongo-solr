#! /bin/sh

# A very simple script to start the Mongo shell preloaded with the plugin.
# This script assumes that the Mongo shell is in the executable path.

mongo $* --eval "load('solr-plugin.js')" --shell

