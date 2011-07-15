require_relative "../test_helper"
require "#{PROJ_SRC_PATH}/config_format_reader"

class ConfigFormatReaderTest < Test::Unit::TestCase
  include MongoSolr

  SAMPLE_SOLR_LOC = "http://localhost::8983/solr"
  SAMPLE_ENTRIES =
  [
   {
     SolrConfigConst::SOLR_URL_KEY => SAMPLE_SOLR_LOC,
     SolrConfigConst::NS_KEY => "courses.undergrad",
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SAMPLE_SOLR_LOC,
     SolrConfigConst::NS_KEY => "courses.masters",
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SAMPLE_SOLR_LOC,
     SolrConfigConst::NS_KEY => "courses.doctoral",
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SAMPLE_SOLR_LOC,
     SolrConfigConst::NS_KEY => "staff.prof",
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SAMPLE_SOLR_LOC,
     SolrConfigConst::NS_KEY => "staff.admin",
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   }
  ]

  context "basic test" do
    setup do
      @reader = MongoSolr::ConfigFormatReader.new(SAMPLE_ENTRIES)
    end

    should "extract solr location correctly" do
      assert_equal(SAMPLE_SOLR_LOC, @reader.solr_loc)
    end

    should "extract db_set correctly" do
      db_set = @reader.get_db_set
      assert_equal(2, db_set.size)

      coll = db_set["courses"]
      assert_equal(3, coll.size)
      assert(coll.include? "undergrad")
      assert(coll.include? "masters")
      assert(coll.include? "doctoral")

      coll = db_set["staff"]
      assert_equal(2, coll.size)
      assert(coll.include? "prof")
      assert(coll.include? "admin")
    end    
  end
end

