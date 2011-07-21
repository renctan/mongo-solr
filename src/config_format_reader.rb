require "set"
require_relative "solr_config_const"

module MongoSolr
  # A simple helper class for extracting the configuration information for a Solr Server.
  class ConfigFormatReader
    # [String] the location of the Solr Server
    attr_reader :solr_loc

    # @param config_data [Hash] A single entry of config data for a Solr Server extracted
    #   from the config DB.
    def initialize(config_data)
      @config_data = config_data

      @solr_loc = @config_data[SolrConfigConst::SOLR_URL_KEY]
    end

    # Converts the config data object to a format recognized by
    # MongoSolr::SolrSynchronizer#update_db_set
    #
    # @return [Hash] @see MongoSolr::SolrSynchronizer#update_db_set
    def get_db_set
      db_set = {}

      @config_data[SolrConfigConst::LIST_KEY].each do |ns_entry|
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
  end
end

