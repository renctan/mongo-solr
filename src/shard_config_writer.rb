require_relative "solr_config_const"
require_relative "util"
require_relative "config_writer"

module MongoSolr
  # A simple class for writing updates to the configuration database of a sharded cluster.
  class ShardConfigWriter < ConfigWriter
    include Util

    # @param coll [Mongo::Collection] The configuration database collection.
    # @param logger [Logger] A logger object to use for logging. Can be nil.
    # @param shard_id [String] A unique identifier for a shard.
    # @param solr_loc [String] The location of the Solr Server
    def initialize(coll, logger, shard_id, solr_loc)
      @shard_id = shard_id
      super(coll, logger, solr_loc)
    end

    # @Override
    #
    # Updates the timestamp for the given namespace. Updates will only happen if the
    # entry for the namespace exists. The reason for this is that the given namespace
    # may already have been removed by the client for indexing.
    #
    # @param namespace [String] The whole namespace name of the collection to update.
    # @param new_timestamp [BSON::Timestamp] The new timestamp
    def update_timestamp(namespace, new_timestamp)
      begin
        key = "#{SolrConfigConst::UPDATE_TIMESTAMP_KEY}.#{@shard_id}"

        @coll.update({ SolrConfigConst::SOLR_URL_KEY => @solr_loc,
                       SolrConfigConst::NS_KEY => namespace
                     },
                     { "$set" => { key => new_timestamp }})
      rescue => e
        @logger.error get_full_exception_msg(e) unless @logger.nil?
      end
    end
  end
end

