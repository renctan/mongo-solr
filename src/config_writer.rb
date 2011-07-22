require_relative "solr_config_const"

module MongoSolr
  # A simple class for writing updates to the configuration database.
  class ConfigWriter
    # @param solr_loc [String] The location of the Solr Server
    # @param coll [Mongo::Collection] The configuration database collection.
    # @param logger [Logger] A logger object to use for logging.
    def initialize(solr_loc, coll, logger = nil)
      @solr_loc = solr_loc
      @coll = coll
      @logger = logger
    end

    # Updates the timestamp for the given namespace. Updates will only happen if the
    # entry for the namespace exists. The reason for this is that the given namespace
    # may already have been removed by the client for indexing.
    #
    # @param namespace [String] The whole namespace name of the collection to update.
    # @param new_timestamp [BSON::Timestamp] The new timestamp
    def update_timestamp(namespace, new_timestamp)
      begin
        @coll.update({ SolrConfigConst::SOLR_URL_KEY => @solr_loc,
                       NS_CRIT_KEY => namespace
                     },
                     { "$set" => {
                         TS_UPDATE_KEY => new_timestamp
                       }})
      rescue => e
        @logger.error e.mesage unless @logger.nil?
      end
    end

    # Updates the timestamp when a commit is performed.
    #
    # @param timestamp {BSON::Timestamp} The new timestamp
    def update_commit_timestamp(timestamp)
      begin
        @coll.update({ SolrConfigConst::SOLR_URL_KEY => @solr_loc },
                     { "$set" => { SolrConfigConst::TIMESTAMP_KEY => timestamp }})
      rescue => e
        @logger.error e.mesage unless @logger.nil?
      end
    end

    private
    NS_CRIT_KEY = "#{SolrConfigConst::LIST_KEY}.#{SolrConfigConst::NS_KEY}"
    TS_UPDATE_KEY = "#{SolrConfigConst::LIST_KEY}.$.#{SolrConfigConst::TIMESTAMP_KEY}"
  end
end

