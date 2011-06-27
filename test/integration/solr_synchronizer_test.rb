require_relative "../test_helper"
require "#{PROJ_SRC_PATH}/solr_synchronizer"
require "#{PROJ_SRC_PATH}/database"
require "#{PROJ_SRC_PATH}/synchronized_hash"
require "#{PROJ_SRC_PATH}/synchronized_set"

class SolrSynchronizerTest < Test::Unit::TestCase
  DB_LOC = "localhost"
  DB_PORT = 27017
  DB_CONNECTION = Mongo::Connection.new(DB_LOC, DB_PORT)
  TEST_DB = "MongoSolrSynchronizerIntegrationTestDB"
  TEST_DB_2 = "#{TEST_DB}_2"
  MODE = :auto
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
      solr.sync { break }
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
          break
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
          break
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
          break
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
          break
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
          break
        end
      end
    end

    context "selective indexing" do
      setup do
        @db = DB_CONNECTION.db(TEST_DB)
        @db2 = DB_CONNECTION.db(TEST_DB_2)

        @db_set_coll1 = MongoSolr::SynchronizedHash.new
        db_coll = MongoSolr::SynchronizedSet.new
        db_coll.add(@test_coll1.name)
        @db_set_coll1[@db.name] = db_coll
      end

      context "pre-defined collection set" do
        should "perform update on collection in the list (single db)" do
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1)
          solr.logger = DEFAULT_LOGGER

          @solr.expects(:add).once
          @solr.expects(:commit).twice

          solr.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @test_coll1.insert({ "lang" => "Ruby" })
            elsif mode == :sync then
              break
            end
          end
        end

        should "perform update on collection in the list (2 db)" do
          db2_coll = MongoSolr::SynchronizedSet.new
          db2_coll.add(@test_coll3.name)
          @db_set_coll1[@db2.name] = db2_coll
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1)
          solr.logger = DEFAULT_LOGGER

          @solr.expects(:add).twice
          @solr.expects(:commit).times(2..3)

          solr.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @test_coll1.insert({ "lang" => "Ruby" })
              @test_coll3.insert({ "lang" => "Ruby" })
            elsif mode == :sync then
              break
            end
          end
        end

        should "perform update on collection in the list (same db, diff coll)" do
          @db_set_coll1[@db.name].add(@test_coll2.name)

          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1)
          solr.logger = DEFAULT_LOGGER

          @solr.expects(:add).twice
          @solr.expects(:commit).times(2..3)

          solr.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @test_coll1.insert({ "lang" => "Ruby" })
              @test_coll2.insert({ "auth" => "Matz" })
            elsif mode == :sync then
              break
            end
          end
        end

        should "not perform update on collection not in the list (different db)" do
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1)
          solr.logger = DEFAULT_LOGGER

          @solr.expects(:add).never
          @solr.stubs(:commit)

          solr.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @test_coll3.insert({ "lang" => "Ruby" })
            elsif mode == :sync then
              break
            end
          end
        end

        should "not perform update on collection not in the list (same db, diff coll)" do
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1)
          solr.logger = DEFAULT_LOGGER

          @solr.expects(:add).never
          @solr.stubs(:commit)

          solr.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @test_coll2.insert({ "lang" => "Ruby" })
            elsif mode == :sync then
              break
            end
          end
        end
      end

      context "dynamic collection set modification" do
        should "perform update after being added in the list (single db)" do
          db_set = MongoSolr::SynchronizedHash.new
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, db_set)
          solr.logger = DEFAULT_LOGGER

          @solr.expects(:add).once
          @solr.stubs(:commit)

          solr.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @test_coll1.insert({ "lang" => "Ruby" })
            elsif mode == :sync and doc_count == 0 then
              coll_set = MongoSolr::SynchronizedSet.new
              coll_set.add(@test_coll1.name)
              db_set[TEST_DB] = coll_set

              @test_coll1.insert({ "auth" => "Matz" })
            else
              break
            end
          end
        end

        should "not perform update after being deleted from the list (single db)" do
          db_set = MongoSolr::SynchronizedHash.new
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1)
          solr.logger = DEFAULT_LOGGER

          @solr.expects(:add).once
          @solr.stubs(:commit)

          solr.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @test_coll1.insert({ "lang" => "Ruby" })
            elsif mode == :sync and doc_count == 0 then
              db_set[TEST_DB].delete(@test_coll1.name)
              @test_coll1.insert({ "auth" => "Matz" })
            else
              break
            end
          end
        end
      end
    end
  end
end

