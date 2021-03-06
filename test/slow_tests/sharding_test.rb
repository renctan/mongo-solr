require File.expand_path("../../test_helper", __FILE__)

require "rsolr"
require_relative("../shard_manager")
require_relative("../js_plugin_wrapper")
require "#{PROJ_SRC_PATH}/solr_synchronizer"
require "#{PROJ_SRC_PATH}/util"

class ShardingTest < Test::Unit::TestCase
  include TestHelper
  include MongoSolr
  include MongoSolr::Util

  DB_NAME = "test"
  COLL_NAME = "user"
  NS = DB_NAME + "." + COLL_NAME
  SHARD_KEY = "id"
  TIMEOUT = 10
  DEFAULT_LOGGER = Logger.new("/dev/null")

  # Setup a shard cluster with 2 shards.
  #
  # @param shard [ShardManager] The shard manager.
  def self.setup_shard(shard)
    shard.start(2)
    
    mongos = shard.connection
    coll = mongos[DB_NAME].create_collection(COLL_NAME)
    coll.create_index([[SHARD_KEY, Mongo::ASCENDING]])

    db = mongos["admin"]
    db.command({ "enablesharding" => DB_NAME })
    db.command({ "shardcollection" => NS, "key" => { SHARD_KEY => 1 }})
  end

  # Splits the range of values and assign the new chunk to the nth shard.
  #
  # @param shard [ShardManager] The shard manager instance.
  # @param mid_val [Integer] The split value.
  # @param n [Integer] The nth shard.
  def self.presplit(shard, mid_val, n)
    mongo = shard.connection
    coll = mongo[DB_NAME][COLL_NAME]

    shard_coll = mongo["config"]["shards"]
    shard_dest = shard_coll.find.to_a[n]["_id"]

    admin_db = mongo["admin"]

    admin_db.command({ "split" => NS, "middle" => { SHARD_KEY => mid_val }})
    admin_db.command({ "moveChunk" => NS, "find" => { SHARD_KEY => mid_val },
                       "to" => shard_dest })
  end

  context "basic" do
    setup do
      @cluster = ShardManager.new
      ShardingTest.setup_shard(@cluster)

      @mongos = @cluster.connection
      @coll = @mongos[DB_NAME][COLL_NAME]
      @coll.remove

      @js = JSPluginWrapper.new("localhost", ShardManager::MONGOS_PORT)

      js_code = <<JAVASCRIPT
        MSolr.connect();

        db = db.getSiblingDB("#{DB_NAME}");
        db.#{COLL_NAME}.solrIndex();
JAVASCRIPT
      @js.eval(js_code)

      @solr = RSolr.connect
    end

    teardown do
      @js.eval("MSolr.reset();")
      @solr.delete_by_query("*:*")
      @solr.commit

      @cluster.cleanup
    end

    should "update Solr with updates to 2 different chunks" do
      ShardingTest.presplit(@cluster, 50, 1)
      mock = mock()
      mock.expects(:daemon_end).once

      run_daemon("-p #{ShardManager::MONGOS_PORT}") do |pio|
        @coll.insert({ SHARD_KEY => 7 })
        @coll.insert({ SHARD_KEY => 77 })

        result = retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => "*:*" }})
          count = response["response"]["numFound"]
          count == 2
        end

        assert(result, "Failed to index to Solr within #{TIMEOUT} seconds")
        mock.daemon_end
      end
    end

    should "update Solr when deletion occured" do
      ShardingTest.presplit(@cluster, 50, 1)
      doc = { SHARD_KEY => "for_deletion" }
      doc_id = @coll.insert(doc)
      mock = mock()
      mock.expects(:daemon_end).once

      run_daemon("-p #{ShardManager::MONGOS_PORT}") do |pio|
        @coll.remove(doc)

        solr_doc = nil
        result = retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => "_id:#{doc_id}" }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to index to Solr within #{TIMEOUT} seconds")
        assert(solr_doc[SolrSynchronizer::SOLR_DELETED_FIELD],
               "Document is not marked for deletion: #{solr_doc.inspect}")
        mock.daemon_end
      end
    end

    should "detect new shards" do
      mock = mock()
      mock.expects(:daemon_end).once

      run_daemon("-p #{ShardManager::MONGOS_PORT}") do |pio|
        midval = 60
        doc = { SHARD_KEY => midval }
        doc_id = @coll.insert(doc)

        result = retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => "_id:#{doc_id}" }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        # Not the main test, used only for synchronization
        assert(result, "Failed to index to Solr within #{TIMEOUT} seconds")

        id = @cluster.add_shard
        ShardingTest.presplit(@cluster, midval, id)

        doc_id = @coll.insert(doc.merge({ :val => "duplicate" }))

        result = retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => "_id:#{doc_id}" }})
          solr_doc = response["response"]["docs"].first
          not solr_doc.nil?
        end

        assert(result, "Failed to update Solr within #{TIMEOUT} seconds")
        mock.daemon_end
      end
    end

    context "SolrSynchronizer" do
      setup do
        @config_writer = stub_everything("config_writer")
        @mock_solr = mock()

        @sync_opts = {
          :ns_set => { NS => {} },
          :logger => DEFAULT_LOGGER,
          :is_sharded => true
        }

        100.times { |x| @coll.insert({ SHARD_KEY => x }) }
        @mock_solr.stubs(:add)
        @mock_solr.stubs(:commit)
      end

      should "only dump contents from own shard" do
        ShardingTest.presplit(@cluster, 50, 1)

        mongo = @cluster.shard_connection(0)
        oplog_coll = get_oplog_collection(mongo, :auto)

        solr_sync = SolrSynchronizer.
          new(@mock_solr, @mongos, oplog_coll, @config_writer, @sync_opts)

        # Make sure that migration has completed by inserting a doc (Since the migration
        # process holds a write lock, being able to write to the collection implies that
        # the migration is complete)
        coll = mongo[DB_NAME][COLL_NAME]
        coll.insert({ SHARD_KEY => 11, "bboy" => "bgirl" })
        coll.db.get_last_error

        cursor = coll.find
        doc_count = cursor.count
        shard_key_values = Set.new(cursor.to_a.map { |x| x[SHARD_KEY] })

        @mock_solr.expects(:add).times(doc_count).with do |arg|
          shard_key_values.include?(arg[SHARD_KEY])
        end

        solr_sync.sync { |mode, count| break }
      end

      context "chunk migration" do
        should "not delete docs at FROM shard" do
          mongo = @cluster.shard_connection(0)
          oplog_coll = get_oplog_collection(mongo, :auto)

          solr_sync = SolrSynchronizer.
            new(@mock_solr, @mongos, oplog_coll, @config_writer, @sync_opts)

          solr_sync.sync do |mode, doc_count|
            if mode == :finished_dumping then
              ShardingTest.presplit(@cluster, 50, 1)
              @mock_solr.expects(:add).never
            else
              break
            end
          end
        end

        should "not insert docs at TO shard" do
          mongo = @cluster.shard_connection(1)
          oplog_coll = get_oplog_collection(mongo, :auto)

          solr_sync = SolrSynchronizer.
            new(@mock_solr, @mongos, oplog_coll, @config_writer, @sync_opts)

          solr_sync.sync do |mode, doc_count|
            if mode == :finished_dumping then
              ShardingTest.presplit(@cluster, 50, 1)
              @mock_solr.expects(:add).never
            else
              break
            end
          end
        end
      end
    end
  end
end

