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
        cursor = @coll.find

        loop do
          begin
            doc = cursor.next_document          
          rescue => e
            @logger.error e.message unless @logger.nil?
            return self
          end

          break if doc.nil?
          yield doc
        end
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

