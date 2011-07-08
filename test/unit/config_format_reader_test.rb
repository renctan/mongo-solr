require_relative "../test_helper"
require "#{PROJ_SRC_PATH}/config_format_reader"

class ConfigFormatReaderTest < Test::Unit::TestCase
  SAMPLE_SOLR_LOC = "http://localhost::8983/solr"
  SAMPLE_ENTRY = {
    "url" => SAMPLE_SOLR_LOC,
    "m" => "auto",
    "dbs" => [
              { "n" => "courses",
                "colls" => [{"n" => "undergrad"}, {"n" => "masters"}, {"n" => "doctoral"}]},
              { "n" => "staff",
                "colls" => [{"n" => "prof"}, {"n" => "admin"}]}
             ]
  }

  context "basic test" do
    setup do
      @reader = MongoSolr::ConfigFormatReader.new(SAMPLE_ENTRY)
    end

    should "extract solr location correctly" do
      assert_equal(SAMPLE_SOLR_LOC, @reader.get_solr_loc)
    end

    should "extract mode correctly" do
      assert_equal(:auto, @reader.get_mongo_mode)
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

