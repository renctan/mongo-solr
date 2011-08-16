require "rubygems"
require "fileutils"
require "mongo"
require "singleton"

# A simple class for managing a small shard cluster.
#
# Note: not thread-safe
class ShardManager
  include Singleton

  CONFIG_SERVER_PORT = 20000
  CONFIG_SERVER_DIR = "config"
  MONGOS_PORT = 30000
  SHARD1_PORT = 27017
  SHARD1_DIR = "shard1"
  SHARD2_PORT = 27018
  SHARD2_DIR = "shard2"

  def initialize
    @conf_server_pio = nil
    @mongos_pio = nil
    @shard1_pio = nil
    @shard2_pio = nil
  end

  def start
    if @conf_server_pio.nil? then
      FileUtils.mkdir_p CONFIG_SERVER_DIR
      cmd = "mongod --dbpath #{CONFIG_SERVER_DIR} --port #{CONFIG_SERVER_PORT}"
      @conf_server_pio = IO.popen(cmd)

      # Make sure that the config server can accept connections before proceeding
      begin
        Mongo::Connection.new "localhost", CONFIG_SERVER_PORT
      rescue => e
        sleep 1
        retry
      end

      cmd = "mongos --port #{MONGOS_PORT} --configdb localhost:#{CONFIG_SERVER_PORT}"
      @mongos_pio = IO.popen(cmd)

      FileUtils.mkdir_p SHARD1_DIR
      cmd = "mongod --master --dbpath #{SHARD1_DIR} --port #{SHARD1_PORT}"
      @shard1_pio = IO.popen(cmd)

      FileUtils.mkdir_p SHARD2_DIR
      cmd = "mongod --master --dbpath #{SHARD2_DIR} --port #{SHARD2_PORT}"
      @shard2_pio = IO.popen(cmd)

      # Make sure that the config server and the shards are up and can accept
      # connections before proceeding
      begin
        mongos_conn = Mongo::Connection.new "localhost", MONGOS_PORT
        Mongo::Connection.new "localhost", SHARD1_PORT
        Mongo::Connection.new "localhost", SHARD2_PORT
      rescue
        sleep 1
        retry
      end

      admin_db = mongos_conn["admin"]
      admin_db.command({ "addshard" => "localhost:#{SHARD1_PORT}", "allowLocal" => true })
      admin_db.command({ "addshard" => "localhost:#{SHARD2_PORT}", "allowLocal" => true })
    end
  end

  def stop!
    unless @mongos_pio.nil? then
      terminate @mongos_pio
      @mongos_pio = nil

      terminate @conf_server_pio
      @conf_server_pio = nil

      terminate @shard1_pio
      @shard1_pio = nil

      terminate @shard2_pio
      @shard2_pio = nil
    end
  end

  def cleanup
    stop!
    FileUtils.rm_rf CONFIG_SERVER_DIR
    FileUtils.rm_rf SHARD1_DIR
    FileUtils.rm_rf SHARD2_DIR
  end

  def connection
    Mongo::Connection.new "localhost", MONGOS_PORT
  end

  private
  def terminate(pio)
    Process.kill "TERM", pio.pid
    pio.close
  end
end

