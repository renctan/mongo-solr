require_relative "solr_synchronizer"

module MongoSolr
  # A simple wrapper that abstracts the sync thread and the SolrSynchronizer object.
  #
  # Note: Not thread-safe
  class SolrSyncThread
    extend Forwardable

    def_delegators :@solr, update_db_set

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
    def initialize(mongo, config)
      @mongo = mongo
      @config = config
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
        latest_config = @config.get
        new_solr_sync_set = {}

        latest_config.each do |solr_config|
          url = solr_config.get_solr_loc
          new_db_set = solr_config.get_db_set

          if @solr_sync_list.has_key? url then
            solr_sync = @solr_sync_list[url]
            solr_sync.update_db_set(new_db_set)
            @solr_sync_list.delete url
          else
            solr = RSolr.new(:url => url)
            mode = solr_config.get_mongo_mode
            solr_sync =
              SolrSyncThread.new(SolrSynchronizer.new(solr, @mongo, mode,
                                                      new_db_set, sync_opt))
            solr_sync.start
          end

          new_solr_sync_set[url] = solr_sync
        end

        # Terminate all currently running threads who are not in the new config
        @solr_sync_list.each do |url, solr_thread|
          solr_thread.stop
        end

        @solr_sync_list = new_solr_sync_set
        sleep config_poll_interval # Check the config settings again after a second
      end
    end
  end
end

