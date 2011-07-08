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

    # @return [Symbol] the mode for the Mongo
    def get_mongo_mode
      case @config_data[MONGO_MODE_KEY]
      when "rs" then :repl_set
      when "ms" then :master_slave
      else :auto
      end
    end

    # Converts the config data object to a format recognized by
    # MongoSolr::SolrSynchronizer#update_db_set
    #
    # @return [Hash] @see MongoSolr::SolrSynchronizer#update_db_set
    def get_db_set
      db_set = {}

      @config_data[DB_LIST_KEY].each do |db_entry|
        db_name = db_entry[DB_NAME_KEY]
        coll = Set.new(db_entry[COLL_LIST_KEY].map { |doc| doc[COLL_NAME_KEY] })
        db_set[db_name] = coll
      end

      return db_set
    end

    private
    SOLR_URL_KEY = "url"
    MONGO_MODE_KEY = "m"
    DB_LIST_KEY = "dbs"
    COLL_LIST_KEY = "colls"
    DB_NAME_KEY = "n"
    COLL_NAME_KEY = "n"
  end
end

