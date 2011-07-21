require_relative "../test_helper"
require "#{PROJ_SRC_PATH}/mongodb_config_source"

class ConfigDBFixture
  include MongoSolr

  SOLR_LOC1_CONFIG =
  {
    SolrConfigConst::SOLR_URL_KEY => "http://localhost:8983/solr",
    SolrConfigConst::LIST_KEY =>
    [
     {
       SolrConfigConst::NS_KEY => "courses.undergrad",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "courses.masters",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     }
    ]
  }

  SOLR_LOC2_CONFIG =
  {
    SolrConfigConst::SOLR_URL_KEY => "http://somewhere.out.there:4321/solr",
    SolrConfigConst::LIST_KEY =>
    [
     {
       SolrConfigConst::NS_KEY => "courses.doctoral",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "staff.prof",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "staff.admin",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     }
    ]
  }

  def self.all_config_table
    {
      SOLR_LOC1_CONFIG[SolrConfigConst::SOLR_URL_KEY] => SOLR_LOC1_CONFIG,
      SOLR_LOC2_CONFIG[SolrConfigConst::SOLR_URL_KEY] => SOLR_LOC2_CONFIG
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

