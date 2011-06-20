#! /usr/local/bin/ruby

require_relative "src/solr_synchronizer"

if $0 == __FILE__ then
  SOLR_SERVER = "http://localhost:8983/solr"
  DB_SERVER_LOC = "localhost"

  mongo = Mongo::Connection.new(DB_SERVER_LOC, 27017)
  solr_client = RSolr.connect(:url => SOLR_SERVER)

  solr = MongoSolr::SolrSynchronizer.new(solr_client, mongo)
  solr.sync
end

