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
require_relative "src/config_writer_builder"
require_relative "src/solr_config_const"

if $0 == __FILE__ then
  include MongoSolr

  options = ArgumentParser.parse_options(ARGV)
  mongo = Mongo::Connection.new(options.mongo_loc, options.mongo_port)
  Util.authenticate_to_db(mongo, options.auth)

  logger = Logger.new(STDOUT)

  config_db_name = MongoDBConfigSource.get_config_db_name(mongo)
  config_coll = mongo.db(config_db_name).collection(SolrConfigConst::CONFIG_COLLECTION_NAME)
  config_reader = MongoDBConfigSource.new(config_coll, logger)
  config_writer_builder = ConfigWriterBuilder.new(config_coll, logger)

  daemon_opt = {
    :mode => options.mode,
    :config_poll_interval => options.config_interval,
    :err_retry_interval => options.err_interval,
    :interval => options.interval,
    :logger => logger
  }

  Daemon.run(mongo, config_reader, config_writer_builder, daemon_opt)
end

