require_relative "../test_helper"
require "#{PROJ_SRC_PATH}/solr_synchronizer"

class SolrSynchronizerTest < Test::Unit::TestCase
  DB_CONNECTION = Mongo::Connection.new("localhost", 27017)
  TEST_DB = "MongoSolrSynchronizerIntegrationTestDB"
  TEST_DB_2 = "#{TEST_DB}_2"
  MODE = :master_slave

  context "basic test" do
    setup do
      DB_CONNECTION.drop_database(TEST_DB)
      DB_CONNECTION.drop_database(TEST_DB_2)

      @test_coll1 = DB_CONNECTION.db(TEST_DB).create_collection("test1")
      @test_coll2 = DB_CONNECTION.db(TEST_DB).create_collection("test2")
      @test_coll3 = DB_CONNECTION.db(TEST_DB_2).create_collection("test3")
      @connection = DB_CONNECTION
      @connection.stubs(:database_names).returns([TEST_DB, TEST_DB_2])

      @solr = mock()
    end

    should "dump all db contents to solr" do
      @test_coll1.insert({ "foo" => "a" })
      @test_coll2.insert({ "bar" => "b" })
      @test_coll3.insert({ "test" => "c" })

      @solr.expects(:add).times(3)
      @solr.expects(:commit).at_most_once
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE)
      solr.sync { return }
    end

    should "update db contents to solr after dumping" do
      @solr.expects(:add).at_most_once
      @solr.expects(:commit).times(2)
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE)

      solr.sync do |mode, doc_count|
        if mode == :finished_dumping then
          @test_coll1.insert({ "msg" => "Hello world!" })
        elsif mode == :sync then
          return
        end
      end
    end
  end
end

