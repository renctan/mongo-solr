require File.expand_path("../../test_helper", __FILE__)

require "mongo"
require "rsolr"
require_relative("../shard_manager")
require_relative("../js_plugin_wrapper")

class ShardingTest < Test::Unit::TestCase
  include TestHelper

  DB_NAME = "test"
  COLL_NAME = "user"
  NS = DB_NAME + "." + COLL_NAME
  SHARD_KEY = "id"
  TIMEOUT = 10

  def self.startup
    @@cluster = ShardManager.instance
    ShardingTest.setup_shard(@@cluster)
    ShardingTest.presplit(@@cluster, 50)
  end

  def self.shutdown
    @@cluster.cleanup
  end

  def self.setup_shard(shard)
    shard.start

    begin
      mongos = shard.connection
    rescue => e
      # Keep on retrying to establish connection to mongos
      retry
    end

    coll = mongos[DB_NAME].create_collection(COLL_NAME)
    coll.create_index([[SHARD_KEY, Mongo::ASCENDING]])

    db = mongos["admin"]
    db.command({ "enablesharding" => DB_NAME })
    db.command({ "shardcollection" => NS, "key" => { SHARD_KEY => 1 }})
  end

  def self.presplit(shard, mid_val)
    mongo = shard.connection
    coll = mongo[DB_NAME][COLL_NAME]

    shard_coll = mongo["config"]["shards"]
    shard_dest = shard_coll.find.to_a[1]["_id"]

    admin_db = mongo["admin"]

    admin_db.command({ "split" => NS, "middle" => { SHARD_KEY => mid_val }})
    admin_db.command({ "moveChunk" => NS, "find" => { SHARD_KEY => mid_val },
                       "to" => shard_dest })
  end

  context "basic" do
    setup do
      @mongos = @@cluster.connection
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
      @mock = mock()
      @mock.expects(:daemon_end).once
    end

    teardown do
      @js.eval("MSolr.reset();")
      @solr.delete_by_query("*:*")
      @solr.commit
    end

    should "update Solr with updates to 2 different chunks" do
      run_daemon("-p #{ShardManager::MONGOS_PORT}") do
        @coll.insert({ SHARD_KEY => 7 })
        @coll.insert({ SHARD_KEY => 77 })

        result = retry_until_true(TIMEOUT) do
          response = @solr.select({ :params => { :q => "*:*" }})
          count = response["response"]["numFound"]
          count == 2
        end

        assert(result, "Failed to index to Solr within #{TIMEOUT} seconds")
        @mock.daemon_end
      end
    end
  end
end

