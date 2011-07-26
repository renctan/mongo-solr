require_relative "util"

module MongoSolr
  # A simple decorator class that will catch exceptions on selected RSolr::Client methods
  # and retry again at a later time.
  #
  # Currently supported methods:
  # add, delete_by_id, commit
  class SolrRetryDecorator
    # @param solr [RSolr::Client] The Solr client to wrap.
    # @param retry_interval [number] Time in seconds to wait until retrying a failed invocation.
    # @param logger [Logger] The logger to use when outputting errors encountered.
    def initialize(solr, retry_interval, logger = nil)
      @solr = solr
      @logger = logger
      @interval = retry_interval
    end

    private
    # Defines a method that will execute the given method when called that keeps on
    # retrying when an error occurs.
    #
    # @param method_names [Splat<Symbole>] The list of method names in symbols.
    def self.retryify(*method_names)
      method_names.each do |name|
        define_method(name) do |*args|
          begin
            @solr.send(name, *args)
          rescue => e
            @logger.error Util.get_full_exception_msg(e) unless @logger.nil?
            sleep @interval
            retry
          end
        end
      end
    end

    retryify :add, :delete_by_id, :commit
  end
end

