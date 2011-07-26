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
    ConfigDBFixture::CONFIG2[url_key] => ConfigDBFixture::CONFIG2,
    ConfigDBFixture::CONFIG3[url_key] => ConfigDBFixture::CONFIG3
  }
end

# Simple helper method for getting all the configuration documents for this test.
#
# @return [Array<Hash>] the list of documents
def all_config
  return all_config_table.values.flatten
end

class MongoDBConfigSourceTest < Test::Unit::TestCase
  DB_LOC = "localhost"
  DB_PORT = 27017
  DB_CONNECTION = Mongo::Connection.new(DB_LOC, DB_PORT)
  TEST_DB = "MongoDBConfigSourceIntegrationTestDB"

  context "basic test" do
    setup do
      @test_coll1 = DB_CONNECTION.db(TEST_DB).create_collection("test1")
      all_config.each { |doc| @test_coll1.insert(doc) }
    end

    teardown do
      DB_CONNECTION.drop_database(TEST_DB)
    end

    should "iterate all of the cursor results" do
      data_set = all_config
      solr_set = all_config_table

      config = MongoSolr::MongoDBConfigSource.new(@test_coll1)
      count = 0

      solr_servers = solr_set.keys

      config.each do |doc|
        count += 1
        server_name = doc[MongoSolr::SolrConfigConst::SOLR_URL_KEY]

        assert(solr_servers.include?(server_name))
        solr_servers.delete server_name

        ns_list = solr_set[server_name][MongoSolr::SolrConfigConst::LIST_KEY]
        fixture_docs_ns = ns_list.map do |ns_entry|
          ns_entry[MongoSolr::SolrConfigConst::NS_KEY]
        end

        doc[MongoSolr::SolrConfigConst::LIST_KEY].each do |ns_entry|
          assert(fixture_docs_ns.include?(ns_entry[MongoSolr::SolrConfigConst::NS_KEY]))
        end
      end

      assert_equal(data_set.size, count)
    end
  end
end

