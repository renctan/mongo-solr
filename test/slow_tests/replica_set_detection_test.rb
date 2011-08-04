require File.expand_path("../../test_helper", __FILE__)

require "mongo"
require "#{PROJ_SRC_PATH}/util"
require_relative("../repl_set_manager")

class ReplicaSetDetectionTest < Test::Unit::TestCase
  include MongoSolr::Util

  context "normal server" do
    setup do
      @mongo = MongoStarter.new
      @mongo.start

      # Make sure that the server can accept connections
      begin
        conn = Mongo::Connection.new("localhost", MongoStarter::PORT)
      rescue
        sleep 1
        retry
      end
    end

    teardown do
      @mongo.cleanup
    end

    should "return normal connection" do
      connection = auto_detect_replset("localhost", MongoStarter::PORT)
      assert(connection.is_a?(Mongo::Connection), "Connection is a #{connection.class}!")
    end
  end

  context "replica set server" do
    setup do
      @rs = ReplSetManager.new({ :arbiter_count => 0,
                                 :secondary_count => 2,
                                 :passive_count => 0
                               })
      @rs.start_set
    end

    teardown do
      @rs.cleanup_set
    end

    should "return replset connection" do
      connection = auto_detect_replset(@rs.host, @rs.ports[1])
      assert(connection.is_a?(Mongo::ReplSetConnection),
             "Connection is a #{connection.class}!")
    end
  end
end

