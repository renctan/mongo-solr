module MongoSolr
  # An interface class for the source of config data
  class ConfigSource
    # @param block [Proc(solr_config)] The procedure to run for every Solr configuration
    #   entry. The solr_config should be an instance of MongoSolr::ConfigFormatReader or
    #   responds to all its public methods.
    #
    # @return [MongoSolr::ConfigSource] this object.
    def each(&block)
      raise "each method is not implemented"
    end
  end
end

