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
    # @return [Hash] @see MongoSolr::SolrSynchronizer#update_config(opt[:ns_set])
    def get_ns_set
      ns_set = {}

      @config_data.each do |ns_entry|
        namespace = ns_entry[SolrConfigConst::NS_KEY]
        field_value = ns_entry[SolrConfigConst::COLL_FIELD_KEY]

        if field_value.nil? then
          fields = nil
        else
          fields = field_value.keys
        end

        if ns_set.has_key? namespace then
          ns_set[namespace] << fields unless fields.nil?
        else
          ns_set[namespace] = Set.new(fields)
        end
      end

      return ns_set
    end

    # @return [MongoSolr::CheckpointData] the checkpoint data extracted from the
    #   configuration document.
    def get_checkpoint_data
      data = nil

      unless @config_data.empty? then
        ns_set = {}
        @config_data.each do |ns_entry|
          timestamp = ns_entry[SolrConfigConst::UPDATE_TIMESTAMP_KEY]

          unless timestamp.nil? then
            namespace = ns_entry[SolrConfigConst::NS_KEY]
            ns_set[namespace] = timestamp
          end
        end

        data = CheckpointData.new(@config_data.first[SolrConfigConst::COMMIT_TIMESTAMP_KEY],
                                  ns_set)
      end

      return data
    end
  end
end

