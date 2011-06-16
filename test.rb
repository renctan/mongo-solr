#! /usr/local/bin/ruby

require_relative "mongo_solr.rb"

if $0 == __FILE__ then
  SOLR_SERVER = "http://localhost:8983/solr"
  DB_SERVER_LOC = "localhost"
  OPLOG_POLL_INTERVAL = 1 #sec

  solr = MongoSolr::SolrSynchronizer.new(SOLR_SERVER, DB_SERVER_LOC, { :is_master_slave => true })
  # puts solr.get('select', :params => {:q => "manu:samsung"})

  solr.sync
end

