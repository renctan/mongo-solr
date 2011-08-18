require "rubygems"
require "fileutils"
require "mongo"
require "singleton"

# A simple class for managing a small shard cluster.
#
# Note: not thread-safe
class ShardManager
  ShardInfo = Struct.new(:dir, :port, :pipe_io)

  CONFIG_SERVER_PORT = 20000
  CONFIG_SERVER_DIR = "config"
  MONGOS_PORT = 30000

  # @param init_shards [Integer] The number of shards to setup initially. Will also start
  #   the mongos if > 0.
  def initialize
    @conf_server_pio = nil
    @mongos_pio = nil
    @shards = []

    @next_shard_port = 27017
  end

  def start_mongos
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

      begin
        connection()
      rescue
        sleep 1
        retry
      end
    end
  end

  # Adds a new shard to the cluster.
  #
  # @return [Integer] the id of newly added shard.
  def add_shard
    new_id = @shards.size

    @shards << ShardInfo.new("shard#{new_id}", @next_shard_port, nil)
    start_shard(new_id)

    admin_db = connection["admin"]
    admin_db.command({ "addshard" => "localhost:#{@next_shard_port}", "allowLocal" => true })
    
    @next_shard_port += 1
    return new_id
  end

  # Removes the shard that was last added.
  def remove_last_shard
    shard_no = @shards.size

    unless shard_no.zero? then
      shard_no -= 1
      shard = @shards.last

      connection["admin"].command({ "removeshard" => "localhost:#{shard.port}" })
      stop_shard(shard_no)

      @next_shard_port -= 1
      @shards.pop
    end
  end

  # Starts a shard
  #
  # @param shard_no [Integer] The id of the shard to startup
  # @param wait [Boolean] (false) Blocks until shard can be connected if set to true.
  def start_shard(shard_no)
    shard = @shards[shard_no]

    if shard.pipe_io.nil? then
      FileUtils.mkdir_p shard.dir
      cmd = "mongod --master --dbpath #{shard.dir} --port #{shard.port}"
      shard.pipe_io = IO.popen(cmd)

      begin
        shard_connection(shard_no)
      rescue
        sleep 1
        retry
      end
    end
  end

  # Kills a shard.
  #
  # @param shard_no [Integer] The id of the shard to kill.
  def stop_shard(shard_no)
    shard = @shards[shard_no]

    pio = shard.pipe_io
    unless pio.nil? then
      terminate pio
      shard.pipe_io = nil
    end
  end

  # Starts the shard cluster.
  #
  # @param shards [Integer] The number of shards for the cluster
  def start(shards)
    shards_to_add = shards - @shards.size
    start_mongos

    if shards_to_add < 0 then
      shards_to_add.abs.times { |x| remove_last_shard }
    else
      shards_to_add.times { |x| add_shard }
    end

    @shards.each_index do |id|
      start_shard id
    end
  end

  # Stops all running mongos and mongod processes managed by this object.
  def stop!
    unless @mongos_pio.nil? then
      terminate @mongos_pio
      @mongos_pio = nil

      terminate @conf_server_pio
      @conf_server_pio = nil
    end

    @shards.each_index { |id| stop_shard(id) }
  end

  # Stops and remove all data dirs for mongos, config servers and all shards.
  def cleanup
    stop!
    FileUtils.rm_rf CONFIG_SERVER_DIR
    @shards.each { |shard| FileUtils.rm_rf(shard.dir) }
  end

  # @return [Mongo::Connection] the connection to the mongos.
  def connection
    Mongo::Connection.new "localhost", MONGOS_PORT
  end

  # @param id [Integer] The id of the shard.
  #
  # @return [Mongo::Connection] the connection to the shard.
  def shard_connection(id)
    Mongo::Connection.new "localhost", @shards[id].port
  end

  private
  def terminate(pio)
    Process.kill "TERM", pio.pid
    pio.close
  end
end

