# Assumption: A Solr server is running @ http://localhost:8983/solr
# Warning: Don't use a Solr server with important data as this test will wipe out
#   all it's entire contents

require File.expand_path("../../test_helper", __FILE__)

require "mongo"
require "rsolr"
require "logger"
require "#{PROJ_SRC_PATH}/util"
require "#{PROJ_SRC_PATH}/solr_synchronizer"
require_relative("../repl_set_manager")
require_relative("../js_plugin_wrapper")

class ReplicaSetTest < Test::Unit::TestCase
  include MongoSolr
  include MongoSolr::Util
  include TestHelper

  TEST_DB = "SolrSyncReplicaSetTest"
  DEFAULT_LOGGER = Logger.new("/dev/null")

  def self.startup
    @@rs = ReplSetManager.new({ :arbiter_count => 1,
                                :secondary_count => 1,
                                :passive_count => 0
                              })
    @@rs.start_set
  end

  def self.shutdown
    @@rs.cleanup_set
  end

  context "SolrSynchronizer rollbacks" do
    setup do
      @@rs.restart_killed_nodes(true)

      # Use send to access these private methods. The reason for getting this values now
      # is because get_node_with_state will call ensure up, which will fail if some of the
      # nodes in the set is down. And this limitation is very restrictive in a lot of test
      # scenarios. Need to always get the new value since the primary can be assigned to
      # a different node for every run, especially when the previous run includes killing
      # a primary.
      @primary_node = @@rs.send(:get_node_with_state, 1)
      @secondary_node = @@rs.send(:get_node_with_state, 2)

      @mongo = Mongo::ReplSetConnection.new([@@rs.host, @@rs.ports[0]])
      @mongo.drop_database TEST_DB
      @test_coll = @mongo[TEST_DB]["test"]

      @solr = RSolr.connect

      config_writer = stub_everything("config_writer")
      oplog_coll = get_oplog_collection(@mongo, :repl_set)

      @solr_sync = SolrSynchronizer.
        new(@solr, @mongo, oplog_coll, config_writer,
            { :ns_set => { "#{TEST_DB}.test" => {} }, :logger => DEFAULT_LOGGER })
    end

    teardown do
      @solr.delete_by_query("*:*")
      @solr.commit
    end

    should "rollback updates on Solr after failover to secondary" do
      total_docs_updated = 0
      doc_id = 0
      stage_one_done = false
      stage_two_done = false

      @solr_sync.sync do |mode, count|
        if mode == :finished_dumping then
          doc_id = @test_coll.insert({ :upx => 1 })
          resp = @test_coll.db.get_last_error({ :w => 3, :wtimeout => 5000 })
          assert(resp["ok"], "getLastError failure: #{resp.inspect}")
        elsif mode == :sync then
          total_docs_updated += count

          if (total_docs_updated == 1 and not stage_one_done) then
            @@rs.kill(@secondary_node)

            @test_coll.update({ :upx => 1 }, { "$set" => { :upx => 300, :y => 2 }})
            resp = @test_coll.db.get_last_error({ :wtimeout => 5000 })
            assert(resp["ok"], "getLastError failure: #{resp.inspect}")
            stage_one_done = true
          elsif (total_docs_updated == 2 and not stage_two_done) then
            @@rs.kill(@primary_node)
            @@rs.start(@secondary_node)
            stage_two_done = true
          end
        elsif mode == :cursor_reset then
          break
        end
      end

      solr_doc = @solr.select({ :params => { :q => "_id:#{doc_id}",
                                  :rows => 1 }})
      value = solr_doc["response"]["docs"].first["upx"]

      # Note: all dynamic field values are stored as text type in the Solr Server
      assert_equal("1", value, "Solr query response: #{solr_doc.inspect}")
    end

    should "rollback inserts on Solr after failover to secondary" do
      doc_id = 0
      stage_one_done = false      

      @solr_sync.sync do |mode, count|
        if mode == :finished_dumping then
          @@rs.kill(@secondary_node)
          doc_id = @test_coll.insert({ :inx => 1 })
        elsif mode == :sync and count == 1 and !stage_one_done then
          @@rs.kill(@primary_node)
          @@rs.start(@secondary_node)
          stage_one_done = true
        elsif mode == :cursor_reset then
          break
        end
      end

      solr_doc = @solr.select({ :params => { :q => "_id:#{doc_id}",
                                  :rows => 1 }})
      deleted = solr_doc["response"]["docs"].first[SolrSynchronizer::SOLR_DELETED_FIELD]
      assert(deleted, "Doc not deleted. Solr query response: #{solr_doc.inspect}")
    end

    should "rollback deletes on Solr after failover to secondary" do
      total_docs_updated = 0
      doc_id = 0
      doc = { :delx => 1 }
      stage_one_done = false
      stage_two_done = false

      @solr_sync.sync do |mode, count|
        if mode == :finished_dumping then
          doc_id = @test_coll.insert(doc)
          resp = @test_coll.db.get_last_error({ :w => 3, :wtimeout => 5000 })
          assert(resp["ok"], "getLastError failure: #{resp.inspect}")
        elsif mode == :sync then
          total_docs_updated += count

          if (total_docs_updated == 1 and not stage_one_done) then
            @@rs.kill(@secondary_node)

            @test_coll.remove(doc)
            stage_one_done = true
          elsif (total_docs_updated == 2 and not stage_two_done) then
            @@rs.kill(@primary_node)
            @@rs.start(@secondary_node)
            stage_two_done = true
          end
        elsif mode == :cursor_reset then
          break
        end
      end

      solr_doc = @solr.select({ :params => { :q => "_id:#{doc_id}",
                                  :rows => 1 }})

      deleted = solr_doc["response"]["docs"].first[SolrSynchronizer::SOLR_DELETED_FIELD]

      assert(deleted.nil? || !deleted, "#Deleted is true for #{solr_doc.inspect}")
    end
  end

  SMOKE_TEST_DB = "smoke_test"
  SOLR_TEST_KEY = "MongoSolr_slowTest_smokeTest"
  SOLR_TEST_VALUE = "delete_me"
  SOLR_TEST_Q = "#{SOLR_TEST_KEY}:#{SOLR_TEST_VALUE}"
  TIMEOUT = 10

  # @return [Object] a document template that will be used in every test.
  def default_doc
    { SOLR_TEST_KEY => SOLR_TEST_VALUE }
  end

  context "smoke test" do
    setup do
      @@rs.restart_killed_nodes(true)

      @conn_str = "mongodb://#{@@rs.host}:#{@@rs.ports[0]},#{@@rs.host}:#{@@rs.ports[1]}"
      @mongo = Mongo::ReplSetConnection.new([@@rs.host, @@rs.ports[0]])
      @mongo.drop_database SMOKE_TEST_DB

      @test_coll = @mongo[SMOKE_TEST_DB]["test"]
      @solr = RSolr.connect
      @js = JSPluginWrapper.new(@@rs.host, @@rs.ports[0])
    end

    teardown do
      @solr.delete_by_query(SOLR_TEST_Q)
      @solr.commit
    end

    should "update with normal rs" do
      @js.index_to_solr(SMOKE_TEST_DB, @test_coll.name)
      @test_coll.insert(default_doc.merge({ :x => "hello" }), { :safe => true })

      run_daemon("-d #{@conn_str}") do
        solr_doc = nil

        # This block is just for synchronization purposes and is used to make
        # sure that the daemon has already passed the dumping stage.
        result = retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => SOLR_TEST_Q }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to index to Solr within #{TIMEOUT} seconds")

        query = SOLR_TEST_Q + " AND y:why"
        @test_coll.insert(default_doc.merge({ :y => "why" }), { :safe => true })

        result = retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => query }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to update Solr within #{TIMEOUT} seconds")
      end
    end

    should "be able to update after current primary steps down" do
      @js.index_to_solr(SMOKE_TEST_DB, @test_coll.name)
      @test_coll.insert(default_doc.merge({ :x => "hello" }), { :safe => true })

      run_daemon("-d #{@conn_str}") do
        solr_doc = nil

        # This block is just for synchronization purposes and is used to make
        # sure that the daemon has already passed the dumping stage.
        result = retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => SOLR_TEST_Q }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to index to Solr within #{TIMEOUT} seconds")

        @@rs.kill_primary

        query = SOLR_TEST_Q + " AND y:why"

        begin
          @test_coll.insert(default_doc.merge({ :y => "why" }), { :safe => true })
        rescue
          retry
        end

        rs_timeout = 2 * TIMEOUT
        result = retry_until_true(rs_timeout) do
          response = @solr.select({ :params => { :q => query }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to update Solr within #{rs_timeout} seconds")
      end
    end
  end
end

