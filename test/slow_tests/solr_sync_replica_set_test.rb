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

class SolrSyncReplicaSetTest < Test::Unit::TestCase
  include MongoSolr

  TEST_DB = "SolrSyncReplicaSetTest"
  DEFAULT_LOGGER = Logger.new("/dev/null")

  context "rollbacks" do
    setup do
      @primary_node = 0
      @secondary_node = 1

      @rs = ReplSetManager.new({ :arbiter_count => 1,
                                 :secondary_count => 1,
                                 :passive_count => 0
                               })

      @rs.start_set

      @mongo = Mongo::ReplSetConnection.new([@rs.host, @rs.ports[0]])
      @test_coll = @mongo[TEST_DB]["test"]

      @solr = RSolr.connect

      config_writer = mock()
      config_writer.stubs(:update_timestamp)
      config_writer.stubs(:update_commit_timestamp)
      @solr_sync = SolrSynchronizer.new(@solr, @mongo, config_writer,
                                        { :ns_set => { "#{TEST_DB}.test" => {} },
                                          :logger => DEFAULT_LOGGER })
    end

    teardown do
      @rs.cleanup_set
      @solr.delete_by_query("*:*")
      @solr.commit
    end

    should "rollback updates on Solr after failover to secondary" do
      total_docs_updated = 0
      doc_id = 0

      @solr_sync.sync do |mode, count|
        if mode == :finished_dumping then
          doc_id = @test_coll.insert({ :x => 1 })
          @test_coll.db.get_last_error({ :w => 2 })
        elsif mode == :sync then
          total_docs_updated += count

          if (total_docs_updated == 1) then
            @rs.kill(@secondary_node)

            @test_coll.update({ :x => 1 }, { "$set" => { :y => 2 }})
            @test_coll.db.get_last_error
          elsif (total_docs_updated == 2) then
            @rs.kill(@primary_node)
            @rs.start(@secondary_node)
          end
        elsif mode == :cursor_reset then
          break
        end
      end

      solr_doc = @solr.select({ :params => { :q => "_id:#{doc_id}",
                                  :rows => 1 }})

      # Note: all dynamic field values are stored as text type in the Solr Server
      assert_equal("1", solr_doc["response"]["docs"].first["x"])
    end

    should "rollback inserts on Solr after failover to secondary" do
      doc_id = 0

      @solr_sync.sync do |mode, count|
        if mode == :finished_dumping then
          @rs.kill(@secondary_node)
          doc_id = @test_coll.insert({ :x => 1 })
          @test_coll.db.get_last_error
        elsif mode == :sync and count >= 1 then
          @rs.kill(@primary_node)
          @rs.start(@secondary_node)
        elsif mode == :cursor_reset then
          break
        end
      end

      solr_doc = @solr.select({ :params => { :q => "_id:#{doc_id}",
                                  :rows => 1 }})

      assert(solr_doc["response"]["docs"].first[SolrSynchronizer::SOLR_DELETED_FIELD])
    end

    should "rollback deletes on Solr after failover to secondary" do
      total_docs_updated = 0
      doc_id = 0
      doc = { :x => 1 }

      @solr_sync.sync do |mode, count|
        if mode == :finished_dumping then
          doc_id = @test_coll.insert(doc)
          @test_coll.db.get_last_error({ :w => 2 })
        elsif mode == :sync then
          total_docs_updated += count

          if (total_docs_updated == 1) then
            @rs.kill(@secondary_node)

            @test_coll.remove(doc)
            @test_coll.db.get_last_error
          elsif (total_docs_updated == 2) then
            @rs.kill(@primary_node)
            @rs.start(@secondary_node)
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
end

