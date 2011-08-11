require File.expand_path("../../test_helper", __FILE__)
require_relative "../config_db_fixture"
require "#{PROJ_SRC_PATH}/mongodb_config_source"

# Simple helper method for getting all the config entries for this test.
#
# @return [Hash<String, Hash>] the hash object where the key is the url of the Solr Server
#   and the value is the document for its configuration.
def all_config_table
  url_key = MongoSolr::SolrConfigConst::SOLR_URL_KEY

  {
    ConfigDBFixture::SOLR_LOC_2 => ConfigDBFixture::CONFIG2,
    ConfigDBFixture::SOLR_LOC_3 => ConfigDBFixture::CONFIG3
  }
end

# Simple helper method for getting all the configuration documents for this test.
#
# @return [Array<Hash>] the list of documents
def all_config
  return all_config_table.values.flatten
end

# Simple helper method for getting all the sharding config entries for this test.
#
# @return [Hash<String, Hash>] the hash object where the key is the url of the Solr Server
#   and the value is the document for its configuration.
def all_shard_config_table
  url_key = MongoSolr::SolrConfigConst::SOLR_URL_KEY

  {
    ConfigDBFixture::SOLR_LOC_2 => ConfigDBFixture::SHARD_CONFIG2,
    ConfigDBFixture::SOLR_LOC_3 => ConfigDBFixture::SHARD_CONFIG3
  }
end

# Simple helper method for getting all the configuration documents for the sharding test.
#
# @return [Array<Hash>] the list of documents
def all_shard_config
  return all_config_table.values.flatten
end

class MongoDBConfigSourceTest < Test::Unit::TestCase
  DB_LOC = "localhost"
  DB_PORT = 27017
  DB_CONNECTION = Mongo::Connection.new(DB_LOC, DB_PORT)
  TEST_DB = "MongoDBConfigSourceIntegrationTestDB"

  def setup
    @test_coll1 = DB_CONNECTION[TEST_DB]["test1"]
  end

  def teardown
    DB_CONNECTION.drop_database(TEST_DB)
  end

  context "normal" do
    setup do
      @test_coll1.insert(all_config)
    end
  
    should "iterate all of the cursor results" do
      data_set = all_config
      solr_set = all_config_table

      config = MongoSolr::MongoDBConfigSource.new(@test_coll1)
      count = 0

      solr_servers = solr_set.keys

      config.each do |docs|
        server_name = ""
        fixture_docs_ns = []

        docs.each_index do |idx|
          count += 1
          doc = docs[idx]

          if idx == 0 then
            server_name = doc[MongoSolr::SolrConfigConst::SOLR_URL_KEY]
            assert(solr_servers.include?(server_name), "#{server_name} is not in the list!")
            solr_servers.delete server_name

            ns_list = solr_set[server_name]

            fixture_docs_ns = ns_list.map do |ns_entry|
              ns_entry[MongoSolr::SolrConfigConst::NS_KEY]
            end
          else
            assert_equal(server_name, doc[MongoSolr::SolrConfigConst::SOLR_URL_KEY])
          end

          ns = doc[MongoSolr::SolrConfigConst::NS_KEY]
          assert(fixture_docs_ns.include?(ns), "#{ns} does not exists in #{server_name}!")
        end
      end

      assert_equal(data_set.size, count)
    end
  end

  context "sharding" do
    setup do
      @test_coll1.insert(all_shard_config)
    end

    should "iterate all of the cursor results" do
      data_set = all_shard_config
      solr_set = all_shard_config_table

      config = MongoSolr::MongoDBConfigSource.new(@test_coll1)
      count = 0

      solr_servers = solr_set.keys

      config.each do |docs|
        server_name = ""
        fixture_docs_ns = []

        docs.each_index do |idx|
          count += 1
          doc = docs[idx]

          if idx == 0 then
            server_name = doc[MongoSolr::SolrConfigConst::SOLR_URL_KEY]
            assert(solr_servers.include?(server_name), "#{server_name} is not in the list!")
            solr_servers.delete server_name

            ns_list = solr_set[server_name]

            fixture_docs_ns = ns_list.map do |ns_entry|
              ns_entry[MongoSolr::SolrConfigConst::NS_KEY]
            end
          else
            assert_equal(server_name, doc[MongoSolr::SolrConfigConst::SOLR_URL_KEY])
          end

          ns = doc[MongoSolr::SolrConfigConst::NS_KEY]
          assert(fixture_docs_ns.include?(ns), "#{ns} does not exists in #{server_name}!")
        end
      end

      assert_equal(data_set.size, count)
    end
  end
end

