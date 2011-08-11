require_relative "config_writer"

module MongoSolr
  class ConfigWriterBuilder
    # @param [Mongo::Collection] The collection that contains the configuration documents
    # @param [Logger] The logger to use
    def initialize(config_collection, logger = nil)
      @coll = config_collection
      @logger = logger
    end

    # @param solr_url [String] url of the Solr
    #
    # @return [MongoSolr::ConfigWriter]
    def create_writer(solr_url)
      ConfigWriter.new(@coll, @logger, solr_url)
    end
  end
end

