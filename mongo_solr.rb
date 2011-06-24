#! /usr/local/bin/ruby

# A simple script that indexes the contents of a mongoDB instance to Solr and continiously
# updates it with changes on the database. Run the script with --help for more details on
# possible options.

require_relative "src/solr_synchronizer"
require_relative "src/util"
require_relative "src/argument_parser"

if $0 == __FILE__ then
  options = MongoSolr::ArgumentParser.parse_options(ARGV)
  mongo = Mongo::Connection.new(options.mongo_loc, options.mongo_port)
  solr_client = RSolr.connect(:url => options.solr_server)
  MongoSolr::Util.authenticate_to_db(mongo, options.auth)

  solr = MongoSolr::SolrSynchronizer.new(solr_client, mongo, options.mode)
  solr.sync({ :interval => options.interval })
end

