require_relative "solr_synchronizer"

module MongoSolr
  # A very simple factory class for creating RSolr instances
  class SolrFactory
    def self.new_conn(*args)
      SolrSynchronizer.new(*args)
    end
  end
end
