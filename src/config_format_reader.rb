require "set"

module MongoSolr
  # A simple helper class for reading the contents of a single dcoument from the config
  # database.
  class ConfigFormatReader
    # @param config_data [Hash] The config data of a single document from the config database.
    def initialize(config_data)
      @config_data = config_data
    end

    # @return [String] the location of the Solr Server
    def get_solr_loc
      @config_data[SOLR_URL_KEY]
    end

    # Converts the config data object to a format recognized by
    # MongoSolr::SolrSynchronizer#update_db_set
    #
    # @return [Hash] @see MongoSolr::SolrSynchronizer#update_db_set
    def get_db_set
      db_set = {}

      @config_data[DB_LIST_KEY].each do |db_name, db_entry|
        db_set[db_name] = Set.new(db_entry.keys)
      end

      return db_set
    end

    private
    SOLR_URL_KEY = "url"
    DB_LIST_KEY = "dbs"
  end
end

