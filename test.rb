#! /usr/local/bin/ruby

require_relative "mongo_solr"

if $0 == __FILE__ then
  SOLR_SERVER = "http://localhost:8983/solr"
  DB_SERVER_LOC = "localhost"

  solr = MongoSolr::SolrSynchronizer.new(SOLR_SERVER, DB_SERVER_LOC, { :mode => :master_slave })
  solr.sync
end

