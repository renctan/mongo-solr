require "set"
require_relative "solr_config_const"
require_relative "checkpoint_data"

module MongoSolr
  # A simple helper class for extracting the configuration information for a Solr Server.
  class ConfigFormatReader
    # [String] the location of the Solr Server
    attr_reader :solr_loc

    # @param config_data [Array<Hash>|Hash] The config data for the Solr Server (extracted
    #   from the config DB.
    def initialize(config_data)
      unless config_data.is_a? Array then
        @config_data = [config_data]
      else
        @config_data = config_data
      end

      @solr_loc = @config_data.first[SolrConfigConst::SOLR_URL_KEY]
    end

    # Converts the config data object to a format recognized by
    # MongoSolr::SolrSynchronizer#update_db_set
    #
    # @return [Hash] @see MongoSolr::SolrSynchronizer#update_db_set
    def get_db_set
      db_set = {}

      @config_data.each do |ns_entry|
        namespace = ns_entry[SolrConfigConst::NS_KEY]
        split = namespace.split(".")
        db_name = split.first

        split.delete_at 0
        collection_name = split.join(".")

        if db_set.has_key? db_name then
          db_set[db_name] << collection_name
        else
          db_set[db_name] = Set.new([collection_name])
        end
      end

      return db_set
    end

    # @return [MongoSolr::CheckpointData] the checkpoint data extracted from the
    #   configuration document.
    def get_checkpoint_data
      data = nil

      unless @config_data.empty? then
        data = CheckpointData.new(@config_data.first[SolrConfigConst::COMMIT_TIMESTAMP_KEY])

        @config_data.each do |ns_entry|
          namespace = ns_entry[SolrConfigConst::NS_KEY]
          timestamp = ns_entry[SolrConfigConst::UPDATE_TIMESTAMP_KEY]

          data.set(namespace, timestamp)
        end
      end

      return data
    end
  end
end

