require "rubygems"
require "rsolr"
require "forwardable"

require_relative "solr_synchronizer"
require_relative "config_format_reader"

module MongoSolr
  # A simple wrapper that abstracts the sync thread and the SolrSynchronizer object.
  #
  # Note: Not thread-safe
  class SolrSyncThread
    extend Forwardable

    def_delegators :@solr, :update_db_set

    # @param solr [SolrSynchronizer]
    def initialize(solr)
      @thread = nil
      @solr = solr
    end

    # Starts a new thread performing the sync operation. Does nothing if there is already
    # an existing thread running.
    def start
      if @thread.nil? then
        @thread = Thread.start { @solr.sync }
      end
    end

    # Stops the sync thread.
    def stop
      unless @thread.nil? then
        @solr.stop!
        @thread.join
        @thread = nil
      end
    end
  end

  # A daemon that synchronizes contents of a Mongo instance to several Solr Servers.
  class Daemon
    # @param mongo [Mongo::Connection] A connection to the MongoDB instance.
    # @param config [MongoSolr::ConfigSource] The object that contains the configuration
    #   information for all the different Solr Servers.
    # @param mode [Symbol] @see SolrSynchronizer#new
    def initialize(mongo, config, mode)
      @mongo = mongo
      @config = config
      @mode = mode
      @solr_sync_set = {}
    end

    # Run the daemon. This is a blocking call that runs an infinite loop.
    #
    # @option opt [number] :config_poll_interval (1) Number of seconds before checking for
    #   any changes in the configuration
    # @option opt [Hash] :sync_opt ({}) @see MongoSolr::SolrSynchronizer
    def run(opt = {})
      sync_opt = opt[:sync_opt] || {}
      config_poll_interval = opt[:config_poll_interval] || 1

      loop do
        new_solr_sync_set = {}

        @config.each do |config_data|
          solr_config = ConfigFormatReader.new(config_data)

          url = solr_config.solr_loc
          new_db_set = solr_config.get_db_set

          if @solr_sync_set.has_key? url then
            solr_sync = @solr_sync_set[url]
            solr_sync.update_db_set(new_db_set)
            @solr_sync_set.delete url
          else
            puts "url => #{url}"
            solr = RSolr.connect(:url => url)
            solr_sync =
              SolrSyncThread.new(SolrSynchronizer.new(solr, @mongo, @mode,
                                                      new_db_set, sync_opt))
            solr_sync.start
          end

          new_solr_sync_set[url] = solr_sync
        end

        # Terminate all currently running threads who are not in the new config
        @solr_sync_set.each do |url, solr_thread|
          solr_thread.stop
        end

        @solr_sync_set = new_solr_sync_set
        sleep config_poll_interval # Check the config settings again after a second
      end
    end
  end
end

