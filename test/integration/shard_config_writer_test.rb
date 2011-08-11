require File.expand_path("../../test_helper", __FILE__)
require_relative "../config_db_fixture"
require "#{PROJ_SRC_PATH}/shard_config_writer"

class ShardConfigWriterTest < Test::Unit::TestCase
  DB_LOC = "localhost"
  DB_PORT = 27017
  DB_CONNECTION = Mongo::Connection.new(DB_LOC, DB_PORT)
  TEST_DB = "ConfigWriterTestIntegrationTestDB"
  SAMPLE_DOCS = ConfigDBFixture::SHARD_CONFIG2
  SOLR_LOC = ConfigDBFixture::SOLR_LOC_2
  SHARD_ID = ConfigDBFixture::SHARD_2

  context "basic test" do
    setup do
      @config_coll = DB_CONNECTION.db(TEST_DB).create_collection("config")
      @config_coll.insert(SAMPLE_DOCS)
      @config_writer = MongoSolr::ShardConfigWriter.new(SHARD_ID, @config_coll, nil, SOLR_LOC)
    end

    teardown do
      DB_CONNECTION.drop_database(TEST_DB)
    end

    should "correctly update timestamp for existing entry" do
      ns = SAMPLE_DOCS.first[MongoSolr::SolrConfigConst::NS_KEY]

      timestamp = BSON::Timestamp.new(7777, 10)
      @config_writer.update_timestamp(ns, timestamp)
      @config_coll.db.get_last_error # wait till the update gets reflected

      config_doc = @config_coll.find_one({ MongoSolr::SolrConfigConst::NS_KEY => ns })
      config_ts = config_doc[MongoSolr::SolrConfigConst::UPDATE_TIMESTAMP_KEY][SHARD_ID]
      assert_equal(timestamp, config_ts)
    end

    should "not create a new entry if namespace does not exists" do
      ns = "random.house"

      timestamp = BSON::Timestamp.new(4321, 10)
      @config_writer.update_timestamp(ns, timestamp)
      @config_coll.db.get_last_error # wait till the update gets reflected

      config_doc = @config_coll.find_one({ MongoSolr::SolrConfigConst::NS_KEY => ns })
      assert(config_doc.nil?)
    end

    should "correctly update commit timestamp" do
      timestamp = BSON::Timestamp.new(123789, 10)
      @config_writer.update_commit_timestamp(timestamp)
      @config_coll.db.get_last_error # wait till the update gets reflected

      config_doc = @config_coll.find_one
      result = config_doc[MongoSolr::SolrConfigConst::COMMIT_TIMESTAMP_KEY]
      assert_equal(timestamp, result)
    end
  end
end

