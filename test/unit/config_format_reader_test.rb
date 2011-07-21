require_relative "../test_helper"
require_relative "../config_db_fixture"
require "#{PROJ_SRC_PATH}/config_format_reader"

class ConfigFormatReaderTest < Test::Unit::TestCase
  include MongoSolr

  context "basic test" do
    setup do
      @reader = MongoSolr::ConfigFormatReader.new(ConfigDBFixture::CONFIG1)
    end

    should "extract solr location correctly" do
      assert_equal(ConfigDBFixture::CONFIG1[SolrConfigConst::SOLR_URL_KEY], @reader.solr_loc)
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

