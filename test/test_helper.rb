require "fileutils"

require "rubygems"
gem "test-unit" # Use the test-unit gem instead of the built-in one
require "test/unit"
require "mocha"
require "shoulda"
require "mongo"

# Copy and pasted from:
# http://stackoverflow.com/questions/4333286/ruby-require-vs-require-relative-best-practice-to-workaround-running-in-both-r/4718414#4718414
unless Kernel.respond_to?(:require_relative)
  module Kernel
    def require_relative(path)
      require File.join(File.dirname(caller[0]), path.to_str)
    end
  end
end

require_relative "proj"

# Simple class for starting and stopping a master/slave mongod process.
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
  def start
    if @pipe_io.nil? then
      FileUtils.mkdir_p DATA_DIR
      cmd = "mongod --master --dbpath #{DATA_DIR} --port #{PORT} " + 
        "--logpath /dev/null --quiet --auth"
      @pipe_io = IO.popen(cmd)
    end
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

module TestHelper
  # A simple method to keep on executing the given block until it returns true.
  #
  # @param timeout [Float] Time limit before the retry operation fails.
  # @param block [Proc] The procedure to perform. Should return true to stop retrying.
  #
  # @return [Boolean] true if the block returns true before the timeout elapses.
  def retry_until_true(timeout, &block)
    success = false
    start_time = Time.now
    
    loop do
      result = yield

      if result then
        success = true
        break
      elsif (Time.now - start_time > timeout) then
        break
      end

      sleep 1 # Deschedule self to allow other threads to progress
    end

    return success
  end

  # Run the Mongo-Solr daemon and terminate it.
  #
  # @param args [String] The arguments to pass to the daemon.
  # @param block [Proc(pio)] The procedure to execute before terminating the daemon.
  #   The piped IO to the daemon process is also passed to the block.
  def run_daemon(args = "", &block)
    daemon_pio = IO.popen("ruby #{PROJ_SRC_PATH}/../../bin/msolrd #{args} > /dev/null", "r+")

    begin
      yield daemon_pio if block_given?
    ensure
      Process.kill "TERM", daemon_pio.pid
      daemon_pio.close
    end
  end

  # Helper method for keep on trying to execute a block until there is no exception.
  # This is particularly helpful for waiting for the Mongo instance to finish starting up.
  #
  # @param block [Proc] The block procedure to execute
  def retry_until_ok(&block)
    begin
      yield
    rescue
      sleep 1
      retry
    end
  end
end

