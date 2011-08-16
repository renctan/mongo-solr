require File.expand_path("../../test_helper", __FILE__)
require "#{PROJ_SRC_PATH}/solr_synchronizer"
require "#{PROJ_SRC_PATH}/util"
require "#{PROJ_SRC_PATH}/checkpoint_data"

class SolrSynchronizerTest < Test::Unit::TestCase
  include MongoSolr::Util

  DB_LOC = "localhost"
  DB_PORT = 27017
  DB_CONNECTION = Mongo::Connection.new(DB_LOC, DB_PORT)
  TEST_DB = "MongoSolrSynchronizerIntegrationTestDB"
  TEST_DB_2 = "#{TEST_DB}_2"
  DEFAULT_LOGGER = Logger.new("/dev/null")

  context "basic" do
    setup do
      @test_coll1 = DB_CONNECTION.db(TEST_DB).create_collection("test1")
      @test_coll2 = DB_CONNECTION.db(TEST_DB).create_collection("test2")
      @test_coll3 = DB_CONNECTION.db(TEST_DB_2).create_collection("test3")

      @test_coll1_ns = "#{@test_coll1.db.name}.#{@test_coll1.name}"
      @test_coll2_ns = "#{@test_coll2.db.name}.#{@test_coll2.name}"
      @test_coll3_ns = "#{@test_coll3.db.name}.#{@test_coll3.name}"

      @connection = DB_CONNECTION
      @connection.stubs(:database_names).returns([TEST_DB, TEST_DB_2])

      basic_ns_set = {
        @test_coll1_ns => {},
        @test_coll2_ns => {},
        @test_coll3_ns => {}
      }

      @solr = mock()
      config_writer = mock()
      config_writer.stubs(:update_timestamp)
      config_writer.stubs(:update_commit_timestamp)
      config_writer.stubs(:update_total_dump_count)
      config_writer.stubs(:reset_dump_count)
      config_writer.stubs(:increment_dump_count)

      oplog_coll = get_oplog_collection(DB_CONNECTION, :auto)

      @solr_sync = MongoSolr::SolrSynchronizer.
        new(@solr, @connection, oplog_coll, config_writer,
            { :ns_set => basic_ns_set, :logger => DEFAULT_LOGGER })
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
      @solr.expects(:commit).at_least_once
      @solr_sync.sync { break }
    end

    should "update db insertions to solr after dumping" do
      @solr.stubs(:add)
      @solr.stubs(:commit)

      @solr_sync.sync do |mode, doc_count|
        if mode == :finished_dumping then
          @solr.expects(:add).once
          @solr.expects(:commit).once

          @test_coll1.insert({ "msg" => "Hello world!" })
        elsif mode == :sync then
          break
        end
      end
    end

    should "update multiple db insertions to solr after dumping" do
      @solr.stubs(:add)
      @solr.stubs(:commit)

      @solr_sync.sync do |mode, doc_count|
        if mode == :finished_dumping then
          @solr.expects(:add).twice
          @solr.expects(:commit).times(1..2)

          @test_coll1.insert({ "msg" => "Hello world!" })
          @test_coll2.insert({ "author" => "Matz" })
        elsif mode == :sync and doc_count >= 2 then
          break
        end
      end
    end

    should "update db updates to solr after dumping" do
      @test_coll1.insert({ "msg" => "Hello world!" })

      @solr.stubs(:add)
      @solr.stubs(:commit)

      @solr_sync.sync do |mode, doc_count|
        if mode == :finished_dumping then
          @solr.expects(:add).once
          @solr.expects(:commit).once

          @test_coll1.update({ "msg" => "Hello world!" }, {"$set" => {"from" => "Tim Berners"}})
        elsif mode == :sync then
          break
        end
      end
    end

    should "batch multiple db updates to solr after dumping" do
      @test_coll1.insert({ "msg" => "Hello world!" })
      @test_coll1.insert({ "foo" => "bar?" })

      @solr.stubs(:add)
      @solr.stubs(:commit)

      @solr_sync.sync do |mode, doc_count|
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
      doc = { "msg" => "Hello world!" }
      deleted_id = @test_coll1.insert(doc)

      @solr.stubs(:add)
      @solr.stubs(:commit)

      @solr_sync.sync do |mode, doc_count|
        if mode == :finished_dumping then
          @solr.expects(:add).once.with do |doc_arg|
            doc_arg[MongoSolr::SolrSynchronizer::SOLR_DELETED_FIELD] == true and
              doc_arg["_id"] == deleted_id
          end

          @solr.expects(:commit).once
          @test_coll1.remove({ "msg" => "Hello world!" })
        elsif mode == :sync then
          break
        end
      end
    end

    should "not update after stopped." do
      @solr.expects(:add).never
      @solr.stubs(:commit)

      @solr_sync.sync { @solr_sync.stop! }
      @test_coll1.insert({ "lang" => "Ruby" })
    end

    should "sync after several cycles of start/stop." do
      @solr.expects(:add).never
      @solr.stubs(:commit)

      @solr_sync.sync { @solr_sync.stop! }
      @solr_sync.sync { @solr_sync.stop! }

      @solr.expects(:add).once

      @solr_sync.sync do |mode, doc_count|
        if mode == :finished_dumping then
          @test_coll1.insert({ "msg" => "Hello world!" })
        elsif mode == :sync and doc_count >= 1 then
          break
        end
      end
    end

    context "selective indexing" do
      setup do
        @db = DB_CONNECTION.db(TEST_DB)
        @db2 = DB_CONNECTION.db(TEST_DB_2)

        @ns_set = { @test_coll1_ns => {} }
        @solr_sync.update_config({ :ns_set => @ns_set })
      end

      context "pre-defined collection set" do
        should "update on collection in the list (single db)" do
          @solr.stubs(:commit)
          @solr.expects(:add).once

          @solr_sync.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @solr.expects(:commit).once
              @test_coll1.insert({ "lang" => "Ruby" })
            elsif mode == :sync then
              break
            end
          end
        end

        should "update on collection in the list (2 db)" do
          @ns_set[@test_coll3_ns] = {}
          @solr_sync.update_config({ :ns_set => @ns_set })

          @solr.stubs(:commit)

          @solr_sync.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @solr.expects(:add).twice
              @solr.expects(:commit).times(1..2)

              @test_coll1.insert({ "lang" => "Ruby" })
              @test_coll3.insert({ "lang" => "Ruby" })
            elsif mode == :sync then
              break
            end
          end
        end

        should "update on collection in the list (same db, diff coll)" do
          @ns_set[@test_coll2_ns] = {}
          @solr_sync.update_config({ :ns_set => @ns_set, :wait => true })
          @solr.stubs(:commit)

          @solr_sync.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @solr.expects(:add).twice
              @solr.expects(:commit).times(1..2)

              @test_coll1.insert({ "lang" => "Ruby" })
              @test_coll2.insert({ "auth" => "Matz" })
            elsif mode == :sync then
              break
            end
          end
        end

        should "not update on collection not in the list (different db)" do
          @solr.expects(:add).never
          @solr.stubs(:commit)

          @solr_sync.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @test_coll3.insert({ "lang" => "Ruby" })
            elsif mode == :sync then
              break
            end
          end
        end

        should "not update on collection not in the list (same db, diff coll)" do
          @solr.expects(:add).never
          @solr.stubs(:commit)

          @solr_sync.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @test_coll2.insert({ "lang" => "Ruby" })
            elsif mode == :sync then
              break
            end
          end
        end
      end

      context "dynamic collection set modification" do
        setup do
          @solr_sync.update_config({ :ns_set => @ns_set })
        end

        should "should update on new entry added in the set (same db)" do
          @solr.expects(:add).once
          @solr.stubs(:commit)

          @solr_sync.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @ns_set[@test_coll2_ns] = {}
              @solr_sync.update_config({ :ns_set => @ns_set, :wait => true })
              @test_coll2.insert({ "auth" => "Matz" })
            else
              break
            end
          end
        end

        should "should update on new entry added in the set (different db)" do
          @solr.expects(:add).once
          @solr.stubs(:commit)

          @solr_sync.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @ns_set[@test_coll3_ns] = {}
              @solr_sync.update_config({ :ns_set => @ns_set, :wait => true })
              @test_coll3.insert({ "auth" => "Matz" })
            else
              break
            end
          end
        end

        should "should not update on entry removed from the set" do
          @ns_set[@test_coll2_ns] = {}
          @solr_sync.update_config(@ns_set)

          @solr.expects(:add).never
          @solr.stubs(:commit)

          @solr_sync.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @ns_set.delete(@test_coll2_ns)
              @solr_sync.update_config({ :ns_set => @ns_set, :wait => true })
              @test_coll2.insert({ "auth" => "Matz" })
            else
              break
            end
          end
        end
      end

      context "backlog update" do
        setup do
          @solr_sync.update_config({ :ns_set => @ns_set, :wait => true })
        end

        should "update perform all inserts in the backlog" do
          @solr.stubs(:commit)

          sync_thread = Thread.start do
            finished_inserting = false

            @solr_sync.sync do |mode, doc_count|
              if mode == :finished_dumping then
                @ns_set[@test_coll3_ns] = {}
                @solr_sync.update_config({ :ns_set => @ns_set }) do |add_stage, backlog|
                  if add_stage == :finished_dumping and not finished_inserting then
                    @test_coll3.insert({ "lang" => "Ruby" })
                    @test_coll3.insert({ "auth" => "Matz" })

                    finished_inserting = true
                    @solr.expects(:add).twice
                    true
                  elsif add_stage == :finished_dumping and backlog.size < 2 then
                    Thread.pass
                    true
                  elsif add_stage == :depleted_backlog then
                    @solr_sync.stop!
                  else
                    false
                  end
                end
              end
            end
          end

          sync_thread.join
        end

        should "correctly update inserts after depleting the backlog" do
          @solr.stubs(:commit)

          finished_inserting = false

          @solr_sync.sync do |mode, doc_count|
            if mode == :finished_dumping then
              @ns_set[@test_coll3_ns] = {}
              @solr_sync.update_config({ :ns_set => @ns_set }) do |add_stage, backlog|
                if add_stage == :finished_dumping and not finished_inserting then
                  @test_coll3.insert({ "lang" => "Ruby" })

                  finished_inserting = true
                  @solr.expects(:add).twice
                  true
                elsif add_stage == :finished_dumping and backlog.size < 1 then
                  Thread.pass
                  true
                elsif add_stage == :depleted_backlog then
                  @test_coll3.insert({ "auth" => "Matz" })
                else
                  false
                end
              end
            elsif mode == :sync and doc_count >= 1 then
              break
            end
          end
        end
      end
    end

    context "checkpoint" do
      setup do
        @test_coll1_ns = "#{@test_coll1.db.name}.#{@test_coll1.name}"
      end

      should "start indexing from the checkpoint" do
        @test_coll1.insert({ :x => 1 })
        @test_coll1.update({}, { "$set" => { :x => 2 } })
        @test_coll1.db.get_last_error
        # hack for getting the last oplog timestamp
        timestamp = @solr_sync.send :get_last_oplog_timestamp

        @test_coll1.insert({ :y => "why?" })
        checkpoint_data = MongoSolr::CheckpointData.new(timestamp,
                                                        { @test_coll1_ns => timestamp })

        @solr.expects(:add).once
        @solr.expects(:commit).at_least_once

        @solr_sync.update_config({ :checkpt => checkpoint_data })
        @solr_sync.sync do |mode, count|
          break if mode == :sync
        end
      end

      should "dump all if commit timestamp is not available" do
        @test_coll1.insert({ :x => 1 })
        @test_coll1.insert({ :z => 5 })
        @test_coll1.db.get_last_error
        # hack for getting the last oplog timestamp
        timestamp = @solr_sync.send :get_last_oplog_timestamp
        checkpoint_data = MongoSolr::CheckpointData.new(nil, { @test_coll1_ns => timestamp })

        @solr.expects(:add).twice
        @solr.expects(:commit).at_least_once

        @solr_sync.update_config({ :checkpt => checkpoint_data })
        @solr_sync.sync do |mode, count|
          break if mode == :finished_dumping
        end
      end

      should "dump all if timestamp is too old" do
        @test_coll1.insert({ :x => 1 })
        @test_coll1.insert({ :z => 5 })

        timestamp = BSON::Timestamp.new(1, 1)

        @test_coll1.insert({ :y => "why?" })
        @test_coll1.db.get_last_error
        checkpoint_data = MongoSolr::CheckpointData.new(timestamp,
                                                        { @test_coll1_ns => timestamp })

        @solr.expects(:add).times(3)
        @solr.expects(:commit).at_least_once

        @solr_sync.update_config({ :checkpt => checkpoint_data })
        @solr_sync.sync do |mode, count|
          break if mode == :finished_dumping
        end
      end

      should "update inserts correctly after performing a checkpoint operation" do
        @test_coll1.insert({ :x => 1 })
        @test_coll1.db.get_last_error
        # hack for getting the last oplog timestamp
        timestamp = @solr_sync.send :get_last_oplog_timestamp

        @test_coll1.update({}, { "$set" => { :x => 2 } })
        checkpoint_data = MongoSolr::CheckpointData.new(timestamp,
                                                        { @test_coll1_ns => timestamp })

        @solr.stubs(:add)
        @solr.stubs(:commit)

        @solr_sync.update_config({ :checkpt => checkpoint_data })
        @solr_sync.sync do |mode, count|
          if mode == :finished_dumping then
            @solr.expects(:add).once
            @solr.expects(:commit).at_least_once
            @test_coll1.insert({ :y => "why?" })
          elsif mode == :sync && count >= 1 then
            break
          end
        end
      end

      should "skip operations older than one's timestamp" do
        @test_coll1.insert({ :x => 1 })
        @test_coll1.db.get_last_error

        # hack for getting the last oplog timestamp
        timestamp1 = @solr_sync.send :get_last_oplog_timestamp

        @test_coll1.insert({ :z => 3 })
        @test_coll1.db.get_last_error
        timestamp2 = @solr_sync.send :get_last_oplog_timestamp
        checkpoint_data = MongoSolr::CheckpointData.new(timestamp2,
                                                        { @test_coll1_ns => timestamp1 })

        @solr.expects(:add).once
        @solr.expects(:commit).at_least_once

        @solr_sync.update_config({ :checkpt => checkpoint_data })
        @solr_sync.sync do |mode, count|
          if mode == :finished_dumping then
            break
          end
        end
      end

      should "dump all if timestamp is not available" do
        @test_coll1.insert({ :x => 1 })
        @test_coll1.insert({ :z => 5 })
        @test_coll1.insert({ :y => "why?" })
        @test_coll1.db.get_last_error

        # hack for getting the last oplog timestamp
        timestamp = @solr_sync.send :get_last_oplog_timestamp

        checkpoint_data = MongoSolr::CheckpointData.new(timestamp, { @test_coll1_ns => nil })

        @solr.expects(:add).times(3)
        @solr.expects(:commit).at_least_once

        @solr_sync.update_config({ :checkpt => checkpoint_data })
        @solr_sync.sync do |mode, count|
          break if mode == :finished_dumping
        end
      end

      should "properly update after skipping ops during checkpoint recovery" do
        @test_coll1.insert({ :x => 1 })
        @test_coll1.insert({ :y => 2 })
        @test_coll1.db.get_last_error
        # hack for getting the last oplog timestamp
        timestamp1 = @solr_sync.send :get_last_oplog_timestamp

        @test_coll1.insert({ :z => 3 })
        @test_coll1.db.get_last_error
        timestamp2 = @solr_sync.send :get_last_oplog_timestamp
        checkpoint_data = MongoSolr::CheckpointData.new(timestamp2,
                                                        { @test_coll1_ns => timestamp1 })

        @solr.stubs(:add)
        @solr.stubs(:commit)

        @solr_sync.update_config({ :checkpt => checkpoint_data })
        @solr_sync.sync do |mode, count|
          if mode == :finished_dumping then
            @solr.expects(:add).once.with { |doc_arg| doc_arg["z"] == 5 }
            @solr.expects(:commit).at_least(1)

            @test_coll1.update({ :z => 3 }, { "$set" => { :z => 5 }})
          elsif mode == :sync and count >= 1 then
            break
          end
        end
      end

      should "start indexing from the checkpoint after updating new db_set" do
        @solr.stubs(:commit)

        @solr_sync.sync do |mode, count|
          if mode == :finished_dumping then
            @solr_sync.update_config({ :ns_set => {}, :wait => true } )

            @test_coll1.insert({ :x => 1 })
            @test_coll1.db.get_last_error
            # hack for getting the last oplog timestamp
            timestamp = @solr_sync.send :get_last_oplog_timestamp

            @test_coll1.insert({ :y => "why?" })
            checkpoint_data = MongoSolr::CheckpointData.new(timestamp,
                                                            { @test_coll1_ns => timestamp })

            @solr.expects(:add).once
            @solr.expects(:commit).at_least_once

            @solr_sync.update_config({ :ns_set => { @test_coll1_ns => {} },
                                       :checkpt => checkpoint_data,
                                       :wait => true }) do |update_mode, update_backlog|
              flunk("Should not perform dump!") if update_mode == :finished_dumping
            end

            break
          end
        end
      end

      should "consume backlog after updating new db_set with checkpoint" do
        @solr.stubs(:commit)
        @solr.stubs(:add)
        @test_coll1.insert({ :x => 1 })

        update_thread = nil
        update_proc = Proc.new do |checkpoint_data|
          insert_done = false

          @solr_sync.update_config({ :ns_set => { @test_coll1_ns => {} },
                                     :checkpt => checkpoint_data,
                                     :wait => true }) do |mode, backlog|
            if mode == :finished_dumping then
              flunk("Should not perform dump!")
            elsif mode == :finished_updating then
              if insert_done then
                if backlog.empty? then
                  true
                else
                  false
                end
              else
                @test_coll1.insert({ :z => "why?" })

                @solr.expects(:add).once
                @solr.expects(:commit).at_least_once

                Thread.pass
                insert_done = true
              end
            else
              @solr_sync.stop!
            end
          end
        end

        @solr.stubs(:add)

        @solr_sync.sync do |mode, count|
          if mode == :finished_dumping then
            @test_coll1.db.get_last_error

            # hack for getting the last oplog timestamp
            timestamp = @solr_sync.send :get_last_oplog_timestamp
            checkpoint_data = MongoSolr::CheckpointData.new(timestamp,
                                                            { @test_coll1_ns => timestamp })

            @solr_sync.update_config({ :ns_set => {}, :wait => true } )
            @test_coll1.insert({ :y => 1 })

            update_thread = Thread.start { update_proc.call(checkpoint_data) }
          else
            Thread.pass
          end
        end

        update_thread.join
      end
    end
  end
end

