# Assumption: A Solr server is running @ http://localhost:8983/solr
# Warning: Don't use a Solr server with important data as this test will wipe out
#   all it's entire contents

require File.expand_path("../../test_helper", __FILE__)

require "mongo"
require "rsolr"
require "#{PROJ_SRC_PATH}/util"
require_relative("../repl_set_manager")
require_relative("../js_plugin_wrapper")
require "#{PROJ_SRC_PATH}/mongodb_config_source"
require "#{PROJ_SRC_PATH}/solr_config_const"

# Simple test cases for testing the MongoSolr daemon and the mongo shell plugin.
class SmokeTest < Test::Unit::TestCase
  include MongoSolr
  include TestHelper

  TEST_DB = "smoke_test"
  SOLR_TEST_KEY = "MongoSolr_slowTest_smokeTest"
  SOLR_TEST_VALUE = "delete_me"
  SOLR_TEST_Q = "#{SOLR_TEST_KEY}:#{SOLR_TEST_VALUE}"
  TIMEOUT = 10

  # @return [Object] a document template that will be used in every test.
  def default_doc
    { SOLR_TEST_KEY => SOLR_TEST_VALUE }
  end

  def self.startup
    @@mongo = MongoStarter.new
  end

  def self.shutdown
    @@mongo.cleanup
  end

  context "daemon" do
    DAEMON_ARGS = "-p #{MongoStarter::PORT}"

    setup do
      @@mongo.start

      @mongo_conn = retry_until_ok { Mongo::Connection.new("localhost", MongoStarter::PORT) }
      @mongo_conn.drop_database TEST_DB
      config_db_name = MongoDBConfigSource.get_config_db_name(@mongo_conn)
      @mongo_conn[config_db_name].drop_collection(SolrConfigConst::CONFIG_COLLECTION_NAME)

      @test_coll = @mongo_conn[TEST_DB]["user"]
      @solr = RSolr.connect
      @js = JSPluginWrapper.new("localhost", MongoStarter::PORT)

      @mock = mock()
      @mock.expects(:daemon_end).once
    end

    teardown do
      @solr.delete_by_query(SOLR_TEST_Q)
      @solr.commit
    end

    should "be able to perform dump" do
      @js.index_to_solr(TEST_DB, @test_coll.name)
      @test_coll.insert(default_doc.merge({ :x => "hello" }), { :safe => true })

      run_daemon(DAEMON_ARGS) do
        solr_doc = nil

        result = retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => SOLR_TEST_Q }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to index to Solr within #{TIMEOUT} seconds")
        assert_equal("hello", solr_doc["x"])
        @mock.daemon_end
      end
    end

    should "be able to update from oplog" do
      @js.index_to_solr(TEST_DB, @test_coll.name)
      @test_coll.insert(default_doc.merge({ :x => "hello" }), { :safe => true })

      run_daemon(DAEMON_ARGS) do
        solr_doc = nil

        # This block is just for synchronization purposes and is used to make
        # sure that the daemon has already passed the dumping stage.
        result = retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => SOLR_TEST_Q }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to index to Solr within #{TIMEOUT} seconds")

        query = SOLR_TEST_Q + " AND y:why"
        @test_coll.insert(default_doc.merge({ :y => "why" }), { :safe => true })

        result = retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => query }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to update Solr within #{TIMEOUT} seconds")
        @mock.daemon_end
      end
    end

    should "be able to start indexing after setting a new collection to index" do
      @js.index_to_solr(TEST_DB, @test_coll.name)
      @test_coll.insert(default_doc.merge({ :x => "hello" }), { :safe => true })

      run_daemon(DAEMON_ARGS) do
        solr_doc = nil

        # This block is just for synchronization purposes and is used to make
        # sure that the daemon has already passed the dumping stage.
        result = retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => SOLR_TEST_Q }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to index to Solr within #{TIMEOUT} seconds")

        coll = @mongo_conn["help"]["me"]
        coll.insert(default_doc.merge({ :z => "Zeta" }), { :safe => true })

        @js.index_to_solr(coll.db.name)
        query = SOLR_TEST_Q + " AND z:Zeta"

        result = retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => query }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to update Solr within #{TIMEOUT} seconds")
        @mock.daemon_end
      end
    end
  end

  context "plugin" do
    setup do
      @@mongo.start

      # Make sure that the Mongo instance can already accept connections before proceeding.
      connection = retry_until_ok { Mongo::Connection.new("localhost", MongoStarter::PORT) }

      connection.drop_database TEST_DB
      config_db_name = MongoDBConfigSource.get_config_db_name(connection)
      connection[config_db_name].drop_collection(SolrConfigConst::CONFIG_COLLECTION_NAME)

      @js = JSPluginWrapper.new("localhost", MongoStarter::PORT)
    end

    # A simple test case for checking all the helper methods for the plugin does not
    # cause errors.
    should "not have any error" do
      code = <<JAVASCRIPT
        MSolr.connect();

        db = db.getSiblingDB("sales");
        db.solrIndex();
        db.getSolrIndexes();

        db = db.getSiblingDB("test");
        db.users.help();
        db.users.solrIndex();
        db.users.getSolrIndexes();

        MSolr.showConfig();

        db.dropSolrIndexes();

        db = db.getSiblingDB("sales");
        db.customer.dropSolrIndex();

        MSolr.reset();
JAVASCRIPT

      out = @js.eval(code)
      assert_nil((out =~  /err|exception/i),
                 "Encountered error while executing js:\n#{out}")
    end
  end
end

