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

    def_delegators :@solr, :update_config

    # @param solr [MongoSolr::SolrSynchronizer]
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

  # A simple wrapper that abstracts the run thread and the Daemon object.
  #
  # Note: Not thread-safe
  class DaemonThread
    include Util

    # @param solr [MongoSolr::Daemon]
    def initialize(daemon, host, logger)
      @thread = nil
      @daemon = deamon
      @host = host
      @logger = logger
    end

    # Starts a new thread performing the run operation. Does nothing if there is already
    # an existing thread running.
    def start(*args)
      if @thread.nil? then
        @thread = Thread.start { @daemon.run(*args) }
      end
    end

    # Stops the run thread.
    def stop
      unless @thread.nil? then
        @daemon.stop!
        
        begin
          @thread.join
        rescue => e
          @logger.error("#{@host}: #{get_full_exception_msg(e)}")
        end

        @thread = nil
      end
    end
  end

  # A daemon that synchronizes contents of a Mongo instance to several Solr Servers.
  class Daemon
    def initialize
      @stop = false
      @stop_mutex = Mutex.new
    end

    # Run the daemon. This is a blocking call that runs an infinite loop.
    #
    # @param mongo [Mongo::Connection] A connection to the MongoDB instance.
    # @param oplog_coll [Mongo::Collection] The oplog collection to monitor.
    # @param config_source [MongoSolr::ConfigSource] The object that contains the
    #   configuration information for all the different Solr Servers.
    # @param config_reader_factory [MongoSolr::Factory] A factory for creating
    #   MongoSolr::ConfigFormatReader instances. The create method should be able to accept
    #   one parameter argument that contains the raw configuration data.
    #   @see MongoSolr::ConfigFormatReader
    # @param config_writer_factory [MongoSolr::Factory] A factory for creating
    #   MongoSolr::ConfigWriter instances. The create method should be able to accept a single
    #   String argument that refers to the location of the Solr Server.
    # @option opt [Logger] :logger The object to use for logging. The default logger outputs
    #   to STDOUT.
    # @option opt [number] :config_poll_interval (1) Number of seconds before checking for
    #   any changes in the configuration
    # @option opt [number] :err_retry_interval (10) The interval in seconds to retry again
    #    after encountering an error in the Solr server or MongoDB instance.
    #
    # @see MongoSolr::SolrSynchronizer#new for more recognized values for the opt parameter.
    def run(mongo, oplog_coll, config_source, config_reader_factory,
            config_writer_factory, opt = {})
      @stop_mutex.synchronize { @stop = false }

      config_poll_interval = opt[:config_poll_interval] || 1
      err_retry_interval = opt[:err_retry_interval] || 10
      logger = opt[:logger] || Logger.new(STDOUT)
      solr_sync_set = {}

      loop do
        new_solr_sync_set = {}

        begin
          config_source.each do |config_data|
            solr_config = config_reader_factory.create(config_data)

            url = solr_config.solr_loc
            new_ns_set = solr_config.get_ns_set
            new_checkpoint = solr_config.get_checkpoint_data

            if solr_sync_set.has_key? url then
              solr_sync = solr_sync_set[url]

              solr_sync.update_config({ :ns_set => new_ns_set, :checkpt => new_checkpoint })
              solr_sync_set.delete url
            elsif url_ok?(url, logger) then
              solr = RSolr.connect(:url => url)
              config_writer = config_writer_factory.create(url)

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
            logger.info "#{url} not in new config. Stopping sync thread."
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

        @stop_mutex.synchronize do
          if @stop then
            solr_sync_set.each do |url, solr_thread|
              logger.info "Stopping daemon: Stop sync on #{url}."
              solr_thread.stop
            end

            return
          end
        end
      end
    end

    # Stops the run/run_w_shard thread.
    # Invariant: There should be only one run or run_w_shard thread running on this
    # instance at any moment.
    def stop!
      @stop_mutex.synchronize { @stop = true }
    end

    # Run a set of daemon#run threads on a sharded cluster.
    # @param mongo [Mongo::Connection] A connection to the MongoDB instance.
    # @param oplog_coll [Mongo::Collection] The oplog collection to monitor.
    # @param config_source [MongoSolr::ConfigSource] The object that contains the
    #   configuration information for all the different Solr Servers.
    # @param config_reader_factory [MongoSolr::Factory] A factory for creating
    #   MongoSolr::ConfigFormatReader instances. The create method should be able to accept
    #   one parameter argument that contains the raw configuration data.
    #   @see MongoSolr::ConfigFormatReader
    # @param config_writer_factory [MongoSolr::Factory] A factory for creating
    #   MongoSolr::ConfigWriter instances. The create method should be able to accept a single
    #   String argument that refers to the location of the Solr Server.
    # @option opt [Logger] :logger The object to use for logging. The default logger outputs
    #   to STDOUT.
    #
    # @see MongoSolr::Daemon#run for more recognized values for the opt parameter.
    def self.run_w_shard(mongo, config_source, config_reader_factory,
                         config_writer_factory, opt = {})
      @stop_mutex.synchronize { @stop = false }

      config_poll_interval = opt[:config_poll_interval] || 1
      err_retry_interval = opt[:err_retry_interval] || 10
      logger = opt[:logger] || Logger.new(STDOUT)
      shard_set = {}

      shard_coll = mongo["config"]["shards"]

      loop do
        new_shard_set = {}

        begin
          shard_coll.find.each do |shard_doc|
            shard_id = shard_doc["_id"]
            host = shard_doc["host"]

            if shard_set.has_key? shard_id then
              shard = shard_set[shard_id]
              shard_set.delete(shard_id)
            else
              host_part1, host_part2 = host.split("/")

              if host_part2.nil? then
                # not a replica set
                address = host_part1
              else
                address = host_part2
              end

              location, port = address.split(":")
              shard = DaemonThread.new(Daemon.new(), host, logger)
              shard_conn = auto_detect_replset(location, port)

              if shard_conn.is_a? Mongo::ReplSetConnection then
                oplog_coll = get_oplog_collection(shard_conn, :repl_set)
              else
                oplog_coll = get_oplog_collection(shard_conn, :master_slave)
              end

              shard.start(mongo, oplog_coll, Factory.new(config_reader_factory, shard_id),
                          Factory.new(config_writer_factory, shard_id), opt)
            end

            new_shard_set[shard_id] = shard
          end
        rescue => e
          logger.error get_full_exception_msg(e)
          sleep err_retry_interval
        end

        shard_set.each do |id, daemon_thread|
          logger.info "Shard #{id} removed: stopping sync."
          daemon_thread.stop
        end

        shard_set = new_shard_set

        @stop_mutex.synchronize do
          if @stop then
            shard_set.each do |id, daemon_thread|
              logger.info "Stopping daemon: Stop sync on shard #{id}."
              daemon_thread.stop
            end

            return
          end
        end
      end
    end
  end
end

