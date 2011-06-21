require_relative "../test_helper"
require "#{PROJ_SRC_PATH}/solr_synchronizer"

class SolrSynchronizerTest < Test::Unit::TestCase
  DB_LOC = "localhost"
  DB_PORT = 27017
  DB_CONNECTION = Mongo::Connection.new(DB_LOC, DB_PORT)
  TEST_DB = "MongoSolrSynchronizerIntegrationTestDB"
  TEST_DB_2 = "#{TEST_DB}_2"
  MODE = :master_slave
  DEFAULT_LOGGER = Logger.new("/dev/null")

  context "basic test" do
    setup do
      @test_coll1 = DB_CONNECTION.db(TEST_DB).create_collection("test1")
      @test_coll2 = DB_CONNECTION.db(TEST_DB).create_collection("test2")
      @test_coll3 = DB_CONNECTION.db(TEST_DB_2).create_collection("test3")
      @connection = DB_CONNECTION
      @connection.stubs(:database_names).returns([TEST_DB, TEST_DB_2])

      @solr = mock()
    end

    teardown do
      DB_CONNECTION.drop_database(TEST_DB)
      DB_CONNECTION.drop_database(TEST_DB_2)
    end

    should "dump all db contents to solr" do
      @test_coll1.insert({ "foo" => "a" })
      @test_coll2.insert({ "bar" => "b" })
      @test_coll3.insert({ "test" => "c" })

      @solr.expects(:add).times(3)
      @solr.expects(:commit).once
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE)
      solr.logger = DEFAULT_LOGGER
      solr.sync { return }
    end

    should "update db insertions to solr after dumping" do
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE)
      solr.logger = DEFAULT_LOGGER

      @solr.expects(:add).at_most_once
      @solr.expects(:commit).twice # during and after dump

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
      solr.logger = DEFAULT_LOGGER

      @solr.expects(:add).twice
      @solr.expects(:commit).times(2..3) # during and after dump

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
      solr.logger = DEFAULT_LOGGER

      @solr.expects(:add).twice
      @solr.expects(:commit).twice # during and after dump

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
      solr.logger = DEFAULT_LOGGER

      @solr.stubs(:add)
      @solr.expects(:delete_by_id).once
      @solr.expects(:commit).at_least(2) # during and after dump

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
      solr.logger = DEFAULT_LOGGER

      @solr.expects(:add).at_most(3)
      @solr.expects(:delete_by_id).once
      @solr.expects(:commit).times(2..4) # during and after dump

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

  context "authentication test" do
    ADMIN_DB_CREDENTIALS = { :user => "#{TEST_DB}_admin", :pwd => "admin" }
    LOCAL_DB_CREDENTIALS = { :user => "#{TEST_DB}_local", :pwd => "local" }
    TEST_DB_CREDENTIALS = { :user => "#{TEST_DB}_test", :pwd => "test" }

    setup do
      DB_CONNECTION.db("admin").add_user(ADMIN_DB_CREDENTIALS[:user],
                                         ADMIN_DB_CREDENTIALS[:pwd])
      DB_CONNECTION.db("admin").authenticate(ADMIN_DB_CREDENTIALS[:user],
                                             ADMIN_DB_CREDENTIALS[:pwd])
      DB_CONNECTION.db("local").add_user(LOCAL_DB_CREDENTIALS[:user],
                                         LOCAL_DB_CREDENTIALS[:pwd])
      DB_CONNECTION.db(TEST_DB).add_user(TEST_DB_CREDENTIALS[:user],
                                         TEST_DB_CREDENTIALS[:pwd])

      @test_coll = DB_CONNECTION.db(TEST_DB).create_collection("test")

      # Use a separate connection so authentication will be asked again
      @connection = Mongo::Connection.new(DB_LOC, DB_PORT)
      @connection.stubs(:database_names).returns([TEST_DB])

      @solr = mock()

      @admin_auth = { "admin" => ADMIN_DB_CREDENTIALS }
      @normal_auth = { "local" => LOCAL_DB_CREDENTIALS, TEST_DB => TEST_DB_CREDENTIALS }
    end

    teardown do
      DB_CONNECTION.drop_database(TEST_DB)
      DB_CONNECTION.db("local").remove_user(LOCAL_DB_CREDENTIALS[:user])
      DB_CONNECTION.db("admin").remove_user(ADMIN_DB_CREDENTIALS[:user])
    end

    should "be able to access contents that needs authentication with the admin account" do
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE)
      solr.logger = DEFAULT_LOGGER

      @test_coll.insert({ "msg" => "Hello world!" })

      @solr.expects(:add).once
      @solr.expects(:commit).once

      solr.sync(:db_pass => @admin_auth) { return }
    end

    should "be able to update contents authentication with the admin account after dump" do
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE)
      solr.logger = DEFAULT_LOGGER

      @solr.expects(:add).once
      @solr.expects(:commit).twice # during and after dump

      solr.sync(:db_pass => @admin_auth) do |mode, doc_count|
        if mode == :finished_dumping then
          @test_coll.insert({ "msg" => "Hello world!" })
        elsif mode == :sync then
          return
        end
      end
    end

    should "be able to access contents that needs authentication with individual db" do
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE)
      solr.logger = DEFAULT_LOGGER

      @test_coll.insert({ "msg" => "Hello world!" })

      @solr.expects(:add).once
      @solr.expects(:commit).once

      solr.sync(:db_pass => @normal_auth) { return }
    end

    should "be able to update contents authentication with individual db after dump" do
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE)
      solr.logger = DEFAULT_LOGGER

      @solr.expects(:add).once
      @solr.expects(:commit).twice # during and after dump

      solr.sync(:db_pass => @normal_auth) do |mode, doc_count|
        if mode == :finished_dumping then
          @test_coll.insert({ "msg" => "Hello world!" })
        elsif mode == :sync then
          return
        end
      end
    end
  end
end

