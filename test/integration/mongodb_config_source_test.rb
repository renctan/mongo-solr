require_relative "../test_helper"
require "#{PROJ_SRC_PATH}/mongodb_config_source"

class ConfigDBFixture
  include MongoSolr

  SOLR_LOC_1 = "http://localhost:8983/solr"
  SOLR_LOC1_CONFIG =
  [
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_1,
     SolrConfigConst::NS_KEY => "courses.undergrad",
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_1,
     SolrConfigConst::NS_KEY => "courses.masters",
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   }
  ]

  SOLR_LOC_2 = "http://somewhere.out.there:4321/solr"
  SOLR_LOC2_CONFIG =
  [
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_2,
     SolrConfigConst::NS_KEY => "courses.doctoral",
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_2,
     SolrConfigConst::NS_KEY => "staff.prof",
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_2,
     SolrConfigConst::NS_KEY => "staff.admin",
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   }
  ]

  def self.all_config_table
    {
      SOLR_LOC_1 => SOLR_LOC1_CONFIG,
      SOLR_LOC_2 => SOLR_LOC2_CONFIG
    }
  end

  def self.all_config
    return all_config_table.values.flatten
  end
end

class MongoDBConfigSourceTest < Test::Unit::TestCase
  DB_LOC = "localhost"
  DB_PORT = 27017
  DB_CONNECTION = Mongo::Connection.new(DB_LOC, DB_PORT)
  TEST_DB = "MongoDBConfigSourceIntegrationTestDB"

  context "basic test" do
    setup do
      @test_coll1 = DB_CONNECTION.db(TEST_DB).create_collection("test1")
      ConfigDBFixture.all_config.each { |doc| @test_coll1.insert(doc) }
    end

    teardown do
      DB_CONNECTION.drop_database(TEST_DB)
    end

    should "iterate all of the cursor results" do
      data_set = ConfigDBFixture.all_config
      solr_set = ConfigDBFixture.all_config_table

      config = MongoSolr::MongoDBConfigSource.new(@test_coll1)
      count = 0

      solr_servers = solr_set.keys

      config.each do |docs|
        count = count + docs.size
        server_name = docs.first[MongoSolr::SolrConfigConst::SOLR_URL_KEY]

        assert(solr_servers.include?(server_name))
        solr_servers.delete server_name

        fixture_docs_ns = solr_set[server_name].map do |doc|
          doc[MongoSolr::SolrConfigConst::NS_KEY]
        end

        assert_equal(fixture_docs_ns.size, docs.size)
        docs.each do |doc|
          assert(fixture_docs_ns.include?(doc[MongoSolr::SolrConfigConst::NS_KEY]))
        end
      end

      assert_equal(data_set.size, count)
    end
  end
end

