require File.expand_path("../../test_helper", __FILE__)
require_relative "../config_db_fixture"
require "#{PROJ_SRC_PATH}/config_writer"

class ConfigWriterTest < Test::Unit::TestCase
  DB_LOC = "localhost"
  DB_PORT = 27017
  DB_CONNECTION = Mongo::Connection.new(DB_LOC, DB_PORT)
  TEST_DB = "ConfigWriterTestIntegrationTestDB"
  SAMPLE_DOCS = ConfigDBFixture::CONFIG2
  SOLR_LOC = ConfigDBFixture::SOLR_LOC_2

  context "basic test" do
    setup do
      @config_coll = DB_CONNECTION.db(TEST_DB).create_collection("config")
      @config_coll.insert(SAMPLE_DOCS)
      @config_writer = MongoSolr::ConfigWriter.new(@config_coll, SOLR_LOC)
      @ns = SAMPLE_DOCS.first[MongoSolr::SolrConfigConst::NS_KEY]
    end

    teardown do
      DB_CONNECTION.drop_database(TEST_DB)
    end

    should "correctly update timestamp for existing entry" do
      timestamp = BSON::Timestamp.new(7777, 10)
      @config_writer.update_timestamp(@ns, timestamp)
      @config_coll.db.get_last_error # wait till the update gets reflected

      config_doc = @config_coll.find_one({ MongoSolr::SolrConfigConst::NS_KEY => @ns })
      assert_equal(timestamp, config_doc[MongoSolr::SolrConfigConst::UPDATE_TIMESTAMP_KEY])
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

    should "correctly update the total dump counter" do
      count = 1234567890

      @config_writer.update_total_dump_count(@ns, count)
      @config_coll.db.get_last_error # wait till the update gets reflected

      config_doc = @config_coll.find_one({ MongoSolr::SolrConfigConst::NS_KEY => @ns })
      result = config_doc[MongoSolr::SolrConfigConst::TOTAL_TO_DUMP_KEY]
      assert_equal(count, result)
    end

    should "correctly increment the dump counter" do
      count = 3

      count.times { |x| @config_writer.increment_dump_count(@ns) }
      @config_coll.db.get_last_error # wait till the update gets reflected

      config_doc = @config_coll.find_one({ MongoSolr::SolrConfigConst::NS_KEY => @ns })
      result = config_doc[MongoSolr::SolrConfigConst::DOCS_DUMPED_KEY]
      assert_equal(count, result)
    end

    should "correctly reset the dump counter" do
      count = 3

      count.times { |x| @config_writer.increment_dump_count(@ns) }
      @config_writer.reset_dump_count(@ns)
      @config_coll.db.get_last_error # wait till the update gets reflected

      config_doc = @config_coll.find_one({ MongoSolr::SolrConfigConst::NS_KEY => @ns })
      result = config_doc[MongoSolr::SolrConfigConst::DOCS_DUMPED_KEY]
      assert_equal(0, result)
    end
  end
end

