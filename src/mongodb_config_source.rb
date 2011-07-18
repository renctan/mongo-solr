require_relative "solr_config_const"
require_relative "config_source"
require_relative "config_format_reader"

module MongoSolr
  # A simple class that represents the source of config data coming from a MongoDB instance
  class MongoDBConfigSource < MongoSolr::ConfigSource
    # @param coll [Mongo::Collection] The collection that contains the config data.
    # @param logger [Logger] (nil) The object to use for logging.
    def initialize(coll, logger = nil)
      @coll = coll
      @logger = logger
    end

    # @inheritDoc
    def each(&block)
      if block_given? then
        cursor = @coll.find({}, { :sort => [ SolrConfigConst::SOLR_URL_KEY, :asc ] })
        config_data = []
        current_server = ""

        begin
          doc = cursor.next_document
        rescue => e
          @logger.error e.message unless @logger.nil?
          return self
        end

        stop = false
        while not stop do
          unless doc.nil? then
            new_server = doc[SolrConfigConst::SOLR_URL_KEY]

            if new_server != current_server then
              yield config_data unless config_data.empty?

              config_data.clear
              current_server = new_server
            end

            config_data << doc

            begin
              doc = cursor.next_document
            rescue => e
              @logger.error e.message unless @logger.nil?
              return self
            end
          else
            stop = true
          end
        end

        yield config_data unless config_data.empty?
      end

      return self
    end

    # @param coll [Mongo::Connection] The connection to the MongoDB instance.
    #
    # @return [String] the name of the database that contains the config information.
    def self.get_config_db_name(conn)
      if conn.database_names.include? "config" then
        return "config"
      else
        return "local"
      end
    end
  end
end

