require_relative "../test_helper"
require "#{PROJ_SRC_PATH}/solr_synchronizer"

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

      @basic_db_set = {
        TEST_DB => Set.new(["test1", "test2"]),
        TEST_DB_2 => Set.new(["test3"])
      }

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
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @basic_db_set,
                                             { :logger => DEFAULT_LOGGER })
      solr.sync { break }
    end

    should "update db insertions to solr after dumping" do
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @basic_db_set,
                                             { :logger => DEFAULT_LOGGER })

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
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @basic_db_set,
                                             { :logger => DEFAULT_LOGGER })

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

      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @basic_db_set,
                                             { :logger => DEFAULT_LOGGER })

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

    should "batch multiple db updates to solr after dumping" do
      @test_coll1.insert({ "msg" => "Hello world!" })
      @test_coll1.insert({ "foo" => "bar?" })

      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @basic_db_set,
                                             { :logger => DEFAULT_LOGGER })

      @solr.stubs(:add)
      @solr.stubs(:commit)

      solr.sync do |mode, doc_count|
        if mode == :finished_dumping then
          @test_coll1.update({ "msg" => "Hello world!" }, {"$set" => {"from" => "Tim Berners"}})
          @test_coll1.update({ "foo" => "bar?" }, {"$set" => {"rab" => "oof"}})
          @solr.expects(:add).twice
          @solr.expects(:commit).once
        elsif mode == :sync then
          break
        end
      end
    end

    should "update deleted db contents to solr after dumping" do
      @test_coll1.insert({ "msg" => "Hello world!" })

      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @basic_db_set,
                                             { :logger => DEFAULT_LOGGER })

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

    should "db inserts, updates and deletes to solr after dumping" do
      @test_coll1.insert({ "msg" => "Hello world!" })

      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @basic_db_set,
                                             { :logger => DEFAULT_LOGGER })

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

    should "not update after stopped." do
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @basic_db_set,
                                             { :logger => DEFAULT_LOGGER })

      @solr.expects(:add).never
      @solr.stubs(:commit)

      solr.sync { solr.stop! }
      @test_coll1.insert({ "lang" => "Ruby" })
    end

    should "sync after several cycles of start/stop." do
      solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @basic_db_set,
                                             { :logger => DEFAULT_LOGGER })

      @solr.expects(:add).never
      @solr.stubs(:commit)

      solr.sync { solr.stop! }
      solr.sync { solr.stop! }

      @solr.expects(:add).once

      solr.sync do |mode, doc_count|
        if mode == :finished_dumping then
          @test_coll1.insert({ "msg" => "Hello world!" })
        elsif mode == :sync then
          break
        end
      end
    end

    context "selective indexing" do
      setup do
        @db = DB_CONNECTION.db(TEST_DB)
        @db2 = DB_CONNECTION.db(TEST_DB_2)

        @db_set_coll1 = {}
        @db_set_coll1[@db.name] = Set.new([@test_coll1.name])
      end

      context "pre-defined collection set" do
        should "update on collection in the list (single db)" do
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1,
                                                 { :logger => DEFAULT_LOGGER })

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

        should "update on collection in the list (2 db)" do
          @db_set_coll1[@db2.name] = Set.new([@test_coll3.name])
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1,
                                                 { :logger => DEFAULT_LOGGER })

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

        should "update on collection in the list (same db, diff coll)" do
          @db_set_coll1[@db.name].add(@test_coll2.name)

          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1,
                                                 { :logger => DEFAULT_LOGGER })

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

        should "not update on collection not in the list (different db)" do
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1,
                                                 { :logger => DEFAULT_LOGGER })

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

        should "not update on collection not in the list (same db, diff coll)" do
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1,
                                                 { :logger => DEFAULT_LOGGER })

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

      context "dynamic collection set modification using the add_collection API" do
        should "update after being added in the list (single db)" do
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, {},
                                                 { :logger => DEFAULT_LOGGER })

          @solr.expects(:add).once
          @solr.stubs(:commit)

          solr.sync do |mode, doc_count|
            if mode == :finished_dumping then
              solr.add_collection(TEST_DB, @test_coll1.name, true)
              @test_coll1.insert({ "auth" => "Matz" })
            else
              break
            end
          end
        end

        should "not update after if not in the set calling add_collection (single db)" do
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, {},
                                                 { :logger => DEFAULT_LOGGER })

          @solr.expects(:add).never
          @solr.stubs(:commit)

          solr.sync do |mode, doc_count|
            if mode == :finished_dumping then
              solr.add_collection(TEST_DB, @test_coll2.name, true)
              @test_coll1.insert({ "auth" => "Matz" })
            else
              break
            end
          end
        end
      end

      context "dynamic collection set modification using the update_db_set API" do
        should "should update on new entry added in the set (same db)" do
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1,
                                                 { :logger => DEFAULT_LOGGER })

          @solr.expects(:add).once
          @solr.stubs(:commit)

          solr.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @db_set_coll1[@db.name] << @test_coll2.name
              solr.update_db_set(@db_set_coll1, true)
              @test_coll2.insert({ "auth" => "Matz" })
            else
              break
            end
          end
        end

        should "should update on new entry added in the set (different db)" do
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1,
                                                 { :logger => DEFAULT_LOGGER })

          @solr.expects(:add).once
          @solr.stubs(:commit)

          solr.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @db_set_coll1[@db2.name] = Set.new([@test_coll3.name])
              solr.update_db_set(@db_set_coll1, true)
              @test_coll3.insert({ "auth" => "Matz" })
            else
              break
            end
          end
        end

        should "should not update on entry removed from the set" do
          @db_set_coll1[@db.name] << @test_coll2.name
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1,
                                                 { :logger => DEFAULT_LOGGER })

          @solr.expects(:add).never
          @solr.stubs(:commit)

          solr.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @db_set_coll1[@db.name] = Set.new([@test_coll1.name])
              solr.update_db_set(@db_set_coll1, true)
              @test_coll2.insert({ "auth" => "Matz" })
            else
              break
            end
          end
        end
      end

      context "backlog update testing" do
        should "update perform all inserts in the backlog" do
          solr = MongoSolr::SolrSynchronizer.new(@solr, @connection, MODE, @db_set_coll1,
                                                 { :logger => DEFAULT_LOGGER })

          @solr.stubs(:commit)

          sync_thread = Thread.start do
            finished_inserting = false

            solr.sync do |mode|
              if mode == :finished_dumping then
                solr.add_collection(@db2.name, @test_coll3.name) do |add_stage, backlog|
                  if add_stage == :finished_dumping and not finished_inserting then
                    @test_coll3.insert({ "lang" => "Ruby" })
                    @test_coll3.insert({ "auth" => "Matz" })
                    finished_inserting = true
                    @solr.expects(:add).twice
                    true
                  elsif add_stage == :finished_dumping and backlog.empty? then
                    Thread.pass
                    true
                  elsif add_stage == :depleted_backlog then
                    solr.stop!
                  else
                    false
                  end
                end
              end
            end
          end

          sync_thread.join
        end
      end
    end
  end
end

