require File.expand_path("../../test_helper", __FILE__)

require "mongo"
require "rsolr"

class SmokeTest < Test::Unit::TestCase
  TEST_DB = "smoke_test"
  SOLR_TEST_KEY = "MongoSolr_slowTest_smokeTest"
  SOLR_TEST_VALUE = "delete me"
  SOLR_TEST_Q = "#{SOLR_TEST_KEY}:#{SOLR_TEST_VALUE}"
  TIMEOUT = 5
  JS_DIR = File.expand_path("../smoke_test_js", __FILE__)

  def run_daemon(&block)
    daemon_pio = IO.popen("ruby #{PROJ_SRC_PATH}/../mongo_solr.rb -p #{MongoStarter::PORT}")

    yield if block_given?

    Process.kill "TERM", daemon_pio.pid
    daemon_pio.close
  end

  def run_js(filename)
    orig_path = Dir.pwd
    full_path = JS_DIR + "/#{filename}.js"

    Dir.chdir JS_DIR
    cmd = "mongo --port #{MongoStarter::PORT}" +
      " --eval \"load(\\\"#{full_path}\\\")\""
    res = `#{cmd}`

    Dir.chdir orig_path
  end

  def default_doc
    { SOLR_TEST_KEY => SOLR_TEST_VALUE }
  end

  def setup
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

  def teardown
    @mongo_conn.drop_database(TEST_DB)
    @solr.delete_by_query(SOLR_TEST_Q)
    @mongo.cleanup
  end

  should "be able to perform dump" do
    run_js("index_smoke_test_user")
    @test_coll.insert(default_doc.merge({ :x => "hello" }), { :safe => true })

    run_daemon do
      solr_doc = nil

      result = TestHelper.retry_until_true(TIMEOUT) do
        response = @solr.select({ :params => { :q => SOLR_TEST_Q }})
        solr_doc = response["response"]["docs"].first
        not solr_doc.nil?
      end

      assert(result, "Failed to index to Solr within #{TIMEOUT} seconds")
      assert_equal("hello", solr_doc["x"])
    end
  end
end

