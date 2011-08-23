module MongoSolr
  # An interface class for the source of config data
  class ConfigSource
    # @param block [Proc(Array<Hash>)] The procedure to run for every Solr server
    #   configuration. The array passed to the proc block should contain all the config
    #   documents for a Solr Server.
    #
    # @return [MongoSolr::ConfigSource] this object.
    def each(&block)
      raise "each method is not implemented"
    end
  end
end

