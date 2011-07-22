require_relative "../test_helper"
require_relative "../config_db_fixture"
require "#{PROJ_SRC_PATH}/config_writer"

class ConfigWriterTest < Test::Unit::TestCase
  DB_LOC = "localhost"
  DB_PORT = 27017
  DB_CONNECTION = Mongo::Connection.new(DB_LOC, DB_PORT)
  TEST_DB = "ConfigWriterTestIntegrationTestDB"
  SAMPLE_DOC = ConfigDBFixture::CONFIG2

  context "basic test" do
    setup do
      @config_coll = DB_CONNECTION.db(TEST_DB).create_collection("config")
      @config_coll.insert(SAMPLE_DOC)

      url_key = MongoSolr::SolrConfigConst::SOLR_URL_KEY
      @config_writer = MongoSolr::ConfigWriter.new(SAMPLE_DOC[url_key],
                                                   @config_coll)
    end

    teardown do
      DB_CONNECTION.drop_database(TEST_DB)
    end

    should "correctly update timestamp for existing entry" do
      ns = SAMPLE_DOC[MongoSolr::SolrConfigConst::LIST_KEY].
        first[MongoSolr::SolrConfigConst::NS_KEY]

      timestamp = BSON::Timestamp.new(1000, 10)
      @config_writer.update_timestamp(ns, timestamp)
      @config_coll.db.get_last_error # wait till the update gets reflected

      config_doc = @config_coll.find_one
      result = config_doc[MongoSolr::SolrConfigConst::LIST_KEY].reject do |entry|
        entry[MongoSolr::SolrConfigConst::NS_KEY] != ns
      end

      result_ts = result.first[MongoSolr::SolrConfigConst::TIMESTAMP_KEY]
      assert_equal(timestamp, result_ts)
    end

    should "not create a new entry if namespace does not exists" do
      ns = "random.house"

      timestamp = BSON::Timestamp.new(4321, 10)
      @config_writer.update_timestamp(ns, timestamp)
      @config_coll.db.get_last_error # wait till the update gets reflected

      config_doc = @config_coll.find_one
      result = config_doc[MongoSolr::SolrConfigConst::LIST_KEY].reject do |entry|
        entry[MongoSolr::SolrConfigConst::NS_KEY] != ns
      end

      assert(result.empty?)
    end

    should "correctly update commit timestamp" do
      timestamp = BSON::Timestamp.new(123789, 10)
      @config_writer.update_commit_timestamp(timestamp)
      @config_coll.db.get_last_error # wait till the update gets reflected

      config_doc = @config_coll.find_one
      result = config_doc[MongoSolr::SolrConfigConst::TIMESTAMP_KEY]
      assert_equal(timestamp, result)
    end
  end
end

