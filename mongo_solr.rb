#! /usr/local/bin/ruby

# A simple script that runs the daemon that indexes the contents of a mongoDB instance to
# Solr and continiously updates it with changes on the database. Run the script with
# --help for more details on possible options.

require_relative "src/util"
require_relative "src/argument_parser"
require_relative "src/mongodb_config_source"
require_relative "src/daemon"

if $0 == __FILE__ then
  CONFIG_COLL_NAME = "mongo_solr"

  options = MongoSolr::ArgumentParser.parse_options(ARGV)
  mongo = Mongo::Connection.new(options.mongo_loc, options.mongo_port)
  MongoSolr::Util.authenticate_to_db(mongo, options.auth)

  logger = Logger.new(STDOUT)

  config_db_name = MongoSolr::MongoDBConfigSource.get_config_db_name(mongo)
  config_coll = mongo.db(config_db_name).collection(CONFIG_COLL_NAME)
  config_reader = MongoSolr::MongoDBConfigSource.new(config_coll, logger)
  config_writer_builder = MongoSolr::ConfigWriterBuilder.new(config_coll, logger)

  daemon_opt = {
    :mode => options.mode,
    :config_poll_interval => options.config_interval,
    :interval => options.interval,
    :logger => logger
  }

  MongoSolr::Daemon.run(mongo, config_reader, config_writer_builder, daemon_opt)
end

