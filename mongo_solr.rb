#! /usr/local/bin/ruby

# A simple script that runs the daemon that indexes the contents of a mongoDB instance to
# Solr and continiously updates it with changes on the database. Run the script with
# --help for more details on possible options.

if $0 == __FILE__ then
  # Copy and pasted from:
  # http://stackoverflow.com/questions/4333286/ruby-require-vs-require-relative-best-practice-to-workaround-running-in-both-r/4718414#4718414
  unless Kernel.respond_to?(:require_relative)
    module Kernel
      def require_relative(path)
        require File.join(File.dirname(caller[0]), path.to_str)
      end
    end
  end
end

require_relative "src/util"
require_relative "src/daemon"
require_relative "src/argument_parser"
require_relative "src/mongodb_config_source"
require_relative "src/config_writer"
require_relative "src/shard_config_writer"
require_relative "src/config_format_reader"
require_relative "src/solr_config_const"
require_relative "src/object_builder"
require_relative "src/cleanup"

# Checks whether a connection is connected to a mongos process
#
# @param mongo [Mongo::Connection] The connection to check.
#
# @return [Boolean] true if connected to a mongos
def is_mongos?(mongo)
  reply = mongo["admin"].command({ "isdbgrid" => 1 }, { :check_response => false })
  reply["errmsg"].nil?
end

if $0 == __FILE__ then
  include MongoSolr
  include MongoSolr::Util

  options = ArgumentParser.parse_options(ARGV)

  mongo_loc = options.mongo_loc
  connected_to_mongos = false
  connection_opts = {
    :pool_size => options.conn_pool_size,
    :pool_timeout => 5
  }

  if (mongo_loc =~ /^mongodb:\/\//) then
    mongo = Mongo::Connection.from_uri(mongo_loc, connection_opts)
  else
    mongo = Mongo::Connection.new(mongo_loc, options.mongo_port, connection_opts)
    connected_to_mongos = is_mongos?(mongo)
    mongo = upgrade_to_replset(mongo, connection_opts) unless connected_to_mongos
  end

  authenticate_to_db(mongo, options.auth)

  logger = Logger.new(STDOUT)

  daemon_opt = {
    :config_poll_interval => options.config_interval,
    :err_retry_interval => options.err_interval,
    :auto_dump => options.auto_dump,
    :interval => options.interval,
    :logger => logger
  }

  config_db_name = MongoDBConfigSource.get_config_db_name(mongo)
  config_coll = mongo[config_db_name][SolrConfigConst::CONFIG_COLLECTION_NAME]
  config_source = MongoDBConfigSource.new(config_coll, logger)

  daemon = Daemon.new

  if connected_to_mongos then
    puts "Connected to a mongos @ #{mongo.host}:#{mongo.port}"

    daemon_thread = Thread.start do
      daemon.run_w_shard(mongo, config_source,
                         ObjectBuilder.new(ShardConfigFormatReader),
                         ObjectBuilder.new(ShardConfigWriter, config_coll),
                         daemon_opt)
    end
  else
    if mongo.is_a? Mongo::ReplSetConnection then
      puts "Connected to a replica set @ #{mongo.host}:#{mongo.port}"
      oplog_coll = get_oplog_collection(mongo, :repl_set)
    else
      oplog_coll = get_oplog_collection(mongo, :master_slave)
    end

    daemon_thread = Thread.start do
      daemon.run(mongo, oplog_coll, config_source, ObjectBuilder.new(ConfigFormatReader),
                 ObjectBuilder.new(ConfigWriter, config_coll), daemon_opt)
    end
  end

  Thread.start do
    Cleanup.run(config_source, options.cleanup_interval, options.cleanup_old_age)
  end

  exit_handler = Proc.new do
    logger.info("Recieved terminating signal: shutting down the daemon...")
    daemon.stop!
    daemon_thread.join
    logger.info("Daemon terminated.")
    exit
  end

  Signal.trap("SIGTERM", exit_handler)
  Signal.trap("SIGINT", exit_handler)
  # Do not handle SIGKILL, have it as an option to allow user to force terminatation.

  sleep # Do nothing. Wait for SIGTERM/SIGINT.
end

