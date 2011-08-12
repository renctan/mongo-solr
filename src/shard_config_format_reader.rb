require "set"
require_relative "solr_config_const"
require_relative "checkpoint_data"
require_relative "config_format_reader"

module MongoSolr
  # A simple helper class for extracting the configuration information for a Solr Server.
  class ShardConfigFormatReader < ConfigFormatReader
    # @param shard_id [String] A unique identifier for a shard.
    # @param config_data [Array<Hash>|Hash] The config data for the Solr Server (extracted
    #   from the config DB.
    def initialize(shard_id, config_data)
      @shard_id = shard_id
      super(config_data)
    end

    # @Override
    #
    # @return [MongoSolr::CheckpointData] the checkpoint data extracted from the
    #   configuration document.
    def get_checkpoint_data
      data = nil

      unless @config_data.empty? then
        data = CheckpointData.new(@config_data.first[SolrConfigConst::COMMIT_TIMESTAMP_KEY])

        @config_data.each do |ns_entry|
          update_field = ns_entry[SolrConfigConst::UPDATE_TIMESTAMP_KEY]

          unless update_field.nil? then
            timestamp = update_field[@shard_id]

            unless timestamp.nil? then
              namespace = ns_entry[SolrConfigConst::NS_KEY]
              data.set(namespace, timestamp)
            end
          end
        end
      end

      return data
    end
  end
end

