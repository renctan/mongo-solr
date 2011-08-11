# Tests for handling exceptions with Mongo instance failure.

require File.expand_path("../../test_helper", __FILE__)
require "#{PROJ_SRC_PATH}/solr_synchronizer"
require "#{PROJ_SRC_PATH}/util"

class ExceptionHandlingTest < Test::Unit::TestCase
  include MongoSolr::Util
  include TestHelper

  TEST_DB = "MongoSolrExceptionHandlingIntegrationTestDB"
  TEST_DB_2 = "#{TEST_DB}_2"
  MODE = :auto
  DEFAULT_LOGGER = Logger.new("/dev/null")

  def self.startup
    @@mongo = MongoStarter.new
  end

  def self.shutdown
    @@mongo.cleanup
  end

  context "Mongo Connection Failure" do
    setup do
      @@mongo.start

      @connection = retry_until_ok { Mongo::Connection.new("localhost", MongoStarter::PORT) }
      @connection.drop_database TEST_DB
      @connection.drop_database TEST_DB_2
      @connection.stubs(:database_names).returns([TEST_DB, TEST_DB_2])

      @test_coll1 = @connection[TEST_DB]["test1"]
      @test_coll1_ns = "#{TEST_DB}.#{@test_coll1.name}"

      @solr = mock()

      basic_ns_set = {
        @test_coll1_ns => {}
      }

      config_writer = mock()
      config_writer.stubs(:update_timestamp)
      config_writer.stubs(:update_commit_timestamp)

      oplog_coll = get_oplog_collection(@connection, :master_slave)

      @solr_sync = MongoSolr::SolrSynchronizer.
        new(@solr, @connection, oplog_coll, config_writer,
            { :ns_set => basic_ns_set, :logger => DEFAULT_LOGGER })
    end

    should "succesfully dump db contents after failure" do
      @test_coll1.insert({ :foo => "bar"})
      @test_coll1.db.get_last_error
      @@mongo.stop

      @solr.expects(:add).once
      @solr.expects(:commit).at_least(1)

      sync_thread = Thread.start do
        @solr_sync.sync { |mode, count| break if mode == :finished_dumping }
      end

      @@mongo.start
      sync_thread.join
    end

    should "continue updating Solr from the oplog after recovering from connection failure" do
      @solr.stubs(:add)
      @solr.stubs(:commit)

      @solr_sync.sync do |mode, count|
        if mode == :finished_dumping then
          @@mongo.stop
        elsif mode == :excep then
          @@mongo.start

          @solr.expects(:add).once
          @solr.expects(:commit).at_least(1)
          
          retry_until_ok { @test_coll1.insert({ :x => 1 }) }
        elsif mode == :sync and count >= 1 then
          break
        else
          sleep 1
        end
      end
    end

    should "continue dumping after being disconnected" do
      @solr_sync.update_config({ :ns_set => {} })

      @test_coll1.insert({ :x => 1 })
      @test_coll1.db.get_last_error

      @solr.stubs(:commit)

      @solr_sync.sync do |mode, count|
        if mode == :finished_dumping then
          @@mongo.stop

          @solr.expects(:add).once
          @solr.expects(:commit).at_least(1)

          new_config = { :ns_set => { @test_coll1_ns => {} }}
          @solr_sync.update_config(new_config) do |add_coll_mode, backlog|
            @solr_sync.stop!
            false
          end

          @@mongo.start
        end
      end
    end
  end
end

