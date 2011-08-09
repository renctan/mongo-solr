require "rubygems"
require "rsolr"
require "forwardable"

require_relative "solr_synchronizer"
require_relative "config_format_reader"
require_relative "util"

module MongoSolr
  # A simple wrapper that abstracts the sync thread and the SolrSynchronizer object.
  #
  # Note: Not thread-safe
  class SolrSyncThread
    extend Forwardable
    include Util

    def_delegators :@solr, :update_config

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
    # Run the daemon. This is a blocking call that runs an infinite loop.
    #
    # @param mongo [Mongo::Connection] A connection to the MongoDB instance.
    # @param oplog_coll [Mongo::Collection] The oplog collection to monitor.
    # @param config_source [MongoSolr::ConfigSource] The object that contains the
    #   configuration information for all the different Solr Servers.
    # @param config_writer_builder [MongoSolr::ConfigWriterBuilder] The object that can
    #   create writer objects to be used for modifying the configuration.
    # @option mode [Symbol] :mode @see SolrSynchronizer#new
    # @option opt [Logger] :logger The object to use for logging. The default logger outputs
    #   to STDOUT.
    # @option opt [number] :config_poll_interval (1) Number of seconds before checking for
    #   any changes in the configuration
    # @option opt [number] :err_retry_interval (10) The interval in seconds to retry again
    #    after encountering an error in the Solr server or MongoDB instance.
    #
    # @see MongoSolr::SolrSynchronizer#new for more recognized values for the opt parameter.
    def self.run(mongo, oplog_coll, config_source, config_writer_builder, opt = {})
      config_poll_interval = opt[:config_poll_interval] || 1
      err_retry_interval = opt[:err_retry_interval] || 10
      logger = opt[:logger] || Logger.new(STDOUT)
      solr_sync_set = {}

      loop do
        new_solr_sync_set = {}

        begin
          config_source.each do |config_data|
            solr_config = ConfigFormatReader.new(config_data)

            url = solr_config.solr_loc
            new_ns_set = solr_config.get_ns_set
            new_checkpoint = solr_config.get_checkpoint_data

            if solr_sync_set.has_key? url then
              solr_sync = solr_sync_set[url]

              solr_sync.update_config({ :ns_set => new_ns_set, :checkpt => new_checkpoint })
              solr_sync_set.delete url
            elsif url_ok?(url, logger) then
              solr = RSolr.connect(:url => url)
              config_writer = config_writer_builder.create_writer(url)

              opt[:ns_set] = new_ns_set
              opt[:checkpt] = new_checkpoint
              opt[:name] = url

              solr_sync = SolrSyncThread.
                new(SolrSynchronizer.new(solr, mongo, oplog_coll, config_writer, opt))
              solr_sync.start
            else
              solr_sync = nil
            end

            new_solr_sync_set[url] = solr_sync unless solr_sync.nil?
          end

          # Terminate all currently running threads who are not in the new config
          solr_sync_set.each do |url, solr_thread|
            solr_thread.stop
          end

          solr_sync_set = new_solr_sync_set
          sleep config_poll_interval # Check the config settings again later
        rescue OplogException, StaleCursorException
          raise
        rescue => e
          logger.error get_full_exception_msg(e)
          sleep err_retry_interval
        end
      end
    end
  end
end

