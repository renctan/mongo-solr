require "test/unit"
require "shoulda"
require "mocha"
require "mongo"
require_relative "proj"

# Simple class for starting and stopping the mongod process.
#
# Assumptions:
# 1. No other process is using port 27018
class MongoStarter
  DATA_DIR = "data"
  PORT = 27018

  def initialize
    @pipe_io = nil
  end

  # Forks a new process to run mongod.
  #
  # @param wait [Number] The number of seconds to wait before returning. This is helpful
  #   especially for slow machines since it can have problems creating a database connection
  #   if the mongod has not yet fully started.
  def start(wait = 0)
    if @pipe_io.nil? then
      FileUtils.mkdir_p DATA_DIR
      cmd = "mongod --master --dbpath #{DATA_DIR} --port #{PORT} --logpath /dev/null --quiet"
      @pipe_io = IO.popen(cmd)
    end

    sleep wait
  end

  # Stops the mongod process
  def stop
    unless @pipe_io.nil? then
      Process.kill "TERM", @pipe_io.pid
      @pipe_io.close
      @pipe_io = nil
    end
  end

  # Stops the mongod process if it is still running and cleanup any files generated.
  def cleanup
    stop unless @pipe_io.nil?
    FileUtils.rm_rf DATA_DIR
  end
end

