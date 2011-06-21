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

    should "update db insertions to solr after dumping" do
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE)
      @solr.expects(:add).at_most_once
      @solr.expects(:commit).times(2)

      solr.sync do |mode, doc_count|
        if mode == :finished_dumping then
          @test_coll1.insert({ "msg" => "Hello world!" })
        elsif mode == :sync then
          return
        end
      end
    end

    should "update multiple db insertions to solr after dumping" do
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE)
      @solr.expects(:add).twice
      @solr.expects(:commit).times(2..3)

      solr.sync do |mode, doc_count|
        if mode == :finished_dumping then
          @test_coll1.insert({ "msg" => "Hello world!" })
          @test_coll2.insert({ "author" => "Matz" })
        elsif mode == :sync and doc_count >= 2 then
          return
        end
      end
    end

    should "update db updates to solr after dumping" do
      @test_coll1.insert({ "msg" => "Hello world!" })

      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE)
      @solr.expects(:add).twice
      @solr.expects(:commit).times(2)

      solr.sync do |mode, doc_count|
        if mode == :finished_dumping then
          @test_coll1.update({ "msg" => "Hello world!" }, {"$set" => {"from" => "Tim Berners"}})
        elsif mode == :sync then
          return
        end
      end
    end

    should "update deleted db contents to solr after dumping" do
      @test_coll1.insert({ "msg" => "Hello world!" })

      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE)
      @solr.stubs(:add)
      @solr.expects(:delete_by_id).once
      @solr.expects(:commit).at_least(2)

      solr.sync do |mode, doc_count|
        if mode == :finished_dumping then
          @test_coll1.remove({ "msg" => "Hello world!" })
        elsif mode == :sync then
          return
        end
      end
    end

    should "update db inserts, updates and deletes to solr after dumping" do
      @test_coll1.insert({ "msg" => "Hello world!" })

      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE)
      @solr.expects(:add).at_most(3)
      @solr.expects(:delete_by_id).once
      @solr.expects(:commit).times(2..4)

      solr.sync do |mode, doc_count|
        if mode == :finished_dumping then
          @test_coll1.update({ "msg" => "Hello world!" }, {"$set" => {"from" => "Tim Berners"}})
          @test_coll1.remove({ "msg" => "Hello world!" })
          @test_coll1.insert({ "lang" => "Ruby" })
        elsif mode == :sync and doc_count >= 3 then
          return
        end
      end
    end
  end
end

