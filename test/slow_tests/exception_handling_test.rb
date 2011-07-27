# Tests for handling exceptions with Mongo instance failure.

require File.expand_path("../../test_helper", __FILE__)
require "#{PROJ_SRC_PATH}/solr_synchronizer"
require "fileutils"

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

class ExceptionHandlingTest < Test::Unit::TestCase
  TEST_DB = "MongoSolrExceptionHandlingIntegrationTestDB"
  TEST_DB_2 = "#{TEST_DB}_2"
  MODE = :auto
  DEFAULT_LOGGER = Logger.new("/dev/null")

  context "Mongo Connection Failure" do
    setup do
      @mongo = MongoStarter.new
      @mongo.start

      @connection = retry_until_ok { Mongo::Connection.new("localhost", MongoStarter::PORT) }
      @connection.stubs(:database_names).returns([TEST_DB, TEST_DB_2])
      @test_coll1 = @connection.db(TEST_DB).create_collection("test1")

      @solr = mock()

      basic_db_set = {
        TEST_DB => Set.new(["test1", "test2"]),
        TEST_DB_2 => Set.new(["test3"])
      }

      config_writer = mock()
      config_writer.stubs(:update_timestamp)
      config_writer.stubs(:update_commit_timestamp)

      @solr_sync = MongoSolr::SolrSynchronizer.new(@solr, @connection, config_writer,
                                                   { :db_set => basic_db_set,
                                                     :logger => DEFAULT_LOGGER })
    end

    teardown do
      @mongo.cleanup
    end

    should "succesfully dump db contents after failure" do
      @test_coll1.insert({ :foo => "bar"})
      @test_coll1.db.get_last_error
      @mongo.stop

      @solr.expects(:add).once
      @solr.expects(:commit).at_least(1)

      sync_thread = Thread.start do
        @solr_sync.sync { |mode, count| break if mode == :finished_dumping }
      end

      @mongo.start
      sync_thread.join
    end

    should "continue updating Solr from the oplog after recovering from connection failure" do
      @solr.stubs(:add)
      @solr.stubs(:commit)

      @solr_sync.sync do |mode, count|
        if mode == :finished_dumping then
          @mongo.stop
        elsif mode == :excep then
          @mongo.start

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
      @solr_sync.update_db_set({})

      @test_coll1.insert({ :x => 1 })
      @test_coll1.db.get_last_error

      @solr.stubs(:add)
      @solr.stubs(:commit)

      @solr_sync.sync do |mode, count|
        if mode == :finished_dumping then
          @mongo.stop

          @solr.expects(:add).once
          @solr.expects(:commit).at_least(1)

          @solr_sync.add_collection(TEST_DB, "test1", false) do |add_coll_mode, backlog|
            @solr_sync.stop!
            false
          end

          @mongo.start
        end
      end
    end
  end
end

