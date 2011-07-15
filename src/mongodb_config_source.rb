require_relative "solr_config_const"
require_relative "config_source"
require_relative "config_format_reader"

module MongoSolr
  # A simple class that represents the source of config data coming from a MongoDB instance
  class MongoDBConfigSource < MongoSolr::ConfigSource
    # @param coll [Mongo::Collection] The collection that contains the config data.
    def initialize(coll)
      @coll = coll
    end

    # @inheritDoc
    def each(&block)
      if block_given? then
        cursor = @coll.find({}, { :sort => [ SolrConfigConst::SOLR_URL_KEY, :asc ] })
        config_data = []

        doc = cursor.next_document
        current_server = doc[SolrConfigConst::SOLR_URL_KEY]

        unless doc.nil? then
          config_data << doc
        end

        while doc = cursor.next_document do
          new_server = doc[SolrConfigConst::SOLR_URL_KEY]

          if new_server != current_server then
            yield config_data

            config_data.clear
            current_server = new_server
          end

          config_data << doc
        end

        unless config_data.empty? then
          yield config_data
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

