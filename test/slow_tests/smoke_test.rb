require File.expand_path("../../test_helper", __FILE__)

require "mongo"
require "rsolr"

# Simple test cases for testing the MongoSolr daemon and the mongo shell plugin.
class SmokeTest < Test::Unit::TestCase
  TEST_DB = "smoke_test"
  SOLR_TEST_KEY = "MongoSolr_slowTest_smokeTest"
  SOLR_TEST_VALUE = "delete_me"
  SOLR_TEST_Q = "#{SOLR_TEST_KEY}:#{SOLR_TEST_VALUE}"
  TIMEOUT = 10
  SOLR_PLUGIN_JS_FILE = "#{PROJ_SRC_PATH}/../solr-plugin.js"
  SOLR_LOC = "http://localhost:8983/solr/"

  # Execute a javascript source code.
  #
  # @param js_code [String] The javascript source code to execute.
  #
  # @return [String] The output of the javascript code.
  def run_js(js_code)
    cmd = "mongo --port #{MongoStarter::PORT}" +
      " --eval \"load(\\\"#{SOLR_PLUGIN_JS_FILE}\\\");#{js_code}\""
    `#{cmd}`
  end

  # Escapes the given string to correctly execute when passed to the eval option of
  # the mongo shell program.
  #
  # @param code [String] the original javascript code.
  #
  # @return [String] the escaped string.
  def escape_js(code)
    code.gsub(/\"/, "\\\"")
  end

  # Sets the collection for indexing to Solr.
  #
  # @param db [String] The database name of the collection.
  # @param coll [String] The collection name.
  def index_to_solr(db, coll = "")
    if coll.empty? then
      index_line = "db.getSiblingDB(\"#{db}\").solrIndex();"
    else
      index_line = "db.getSiblingDB(\"#{db}\").#{coll}.solrIndex();"
    end

    code = <<JAVASCRIPT
    MSolr.connect("#{SOLR_LOC}");
    #{index_line}
JAVASCRIPT

    run_js(escape_js(code))
  end

  # Run the Mongo-Solr daemon and terminate it.
  #
  # @param block [Proc] The procedure to execute before terminating the daemon.
  def run_daemon(&block)
    daemon_pio = IO.popen("ruby #{PROJ_SRC_PATH}/../mongo_solr.rb" +
                          " -p #{MongoStarter::PORT} 2> /dev/null")

    begin
      yield if block_given?
    ensure
      Process.kill "TERM", daemon_pio.pid
      daemon_pio.close
    end
  end

  # @return [Object] a document template that will be used in every test.
  def default_doc
    { SOLR_TEST_KEY => SOLR_TEST_VALUE }
  end

  context "daemon" do
    setup do
      @mongo = MongoStarter.new
      @mongo.start

      # Make sure that the Mongo instance can already accept connections before proceeding.
      begin
        connection = Mongo::Connection.new("localhost", MongoStarter::PORT)
      rescue
        sleep 1
        retry
      end

      @mongo_conn = Mongo::Connection.new("localhost", MongoStarter::PORT)
      @test_coll = @mongo_conn[TEST_DB]["user"]
      @solr = RSolr.connect
    end

    teardown do
      @solr.delete_by_query(SOLR_TEST_Q)
      @solr.commit
      @mongo.cleanup
    end

    should "be able to perform dump" do
      index_to_solr(TEST_DB, @test_coll.name)
      @test_coll.insert(default_doc.merge({ :x => "hello" }), { :safe => true })

      run_daemon do
        solr_doc = nil

        # This block is just for synchronization purposes and is used to make
        # sure that the daemon has already passed the dumping stage.
        result = TestHelper.retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => SOLR_TEST_Q }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to index to Solr within #{TIMEOUT} seconds")
        assert_equal("hello", solr_doc["x"])
      end
    end

    should "be able to update from oplog" do
      index_to_solr(TEST_DB, @test_coll.name)
      @test_coll.insert(default_doc.merge({ :x => "hello" }), { :safe => true })

      run_daemon do
        solr_doc = nil

        # This block is just for synchronization purposes and is used to make
        # sure that the daemon has already passed the dumping stage.
        result = TestHelper.retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => SOLR_TEST_Q }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to index to Solr within #{TIMEOUT} seconds")

        query = SOLR_TEST_Q + " AND y:why"
        @test_coll.insert(default_doc.merge({ :y => "why" }), { :safe => true })

        result = TestHelper.retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => query }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to update Solr within #{TIMEOUT} seconds")
      end
    end

    should "be able to start indexing after setting a new collection to index" do
      index_to_solr(TEST_DB, @test_coll.name)
      @test_coll.insert(default_doc.merge({ :x => "hello" }), { :safe => true })

      run_daemon do
        solr_doc = nil

        # This block is just for synchronization purposes and is used to make
        # sure that the daemon has already passed the dumping stage.
        result = TestHelper.retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => SOLR_TEST_Q }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to index to Solr within #{TIMEOUT} seconds")

        coll = @mongo_conn["help"]["me"]
        coll.insert(default_doc.merge({ :z => "Zeta" }), { :safe => true })

        index_to_solr(coll.db.name)
        query = SOLR_TEST_Q + " AND z:Zeta"

        result = TestHelper.retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => query }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to update Solr within #{TIMEOUT} seconds")
      end
    end
  end

  context "plugin" do
    setup do
      @mongo = MongoStarter.new
      @mongo.start

      # Make sure that the Mongo instance can already accept connections before proceeding.
      begin
        connection = Mongo::Connection.new("localhost", MongoStarter::PORT)
      rescue
        sleep 1
        retry
      end
    end

    teardown do
      @mongo.cleanup
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

      out = run_js(escape_js(code))
      assert_nil((out =~  /err|exception/i),
                 "Encountered error while executing js:\n#{out}")
    end
  end
end

