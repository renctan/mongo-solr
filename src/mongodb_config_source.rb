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
        cursor = @coll.find()

        while doc = cursor.next_document do
          yield ConfigFormatReader.new(doc)
        end
      end

      return self
    end
  end
end

