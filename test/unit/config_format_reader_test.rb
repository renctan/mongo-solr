require File.expand_path("../../test_helper", __FILE__)
require_relative "../config_db_fixture"
require "#{PROJ_SRC_PATH}/config_format_reader"

class ConfigFormatReaderTest < Test::Unit::TestCase
  include MongoSolr

  CONFIG_DATA = ConfigDBFixture::CONFIG1

  context "basic test" do
    setup do
      @reader = MongoSolr::ConfigFormatReader.new(CONFIG_DATA)
    end

    should "extract solr location correctly" do
      assert_equal(CONFIG_DATA.first[SolrConfigConst::SOLR_URL_KEY], @reader.solr_loc)
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

    should "extract checkpoint data correctly" do
      data = @reader.get_checkpoint_data

      assert_equal(CONFIG_DATA.first[SolrConfigConst::COMMIT_TIMESTAMP_KEY],
                   data.commit_ts)

      count = 0
      data.each do |ns, ts|
        count += 1

        index = CONFIG_DATA.index do |x|
          x[SolrConfigConst::NS_KEY] == ns and
            x[SolrConfigConst::UPDATE_TIMESTAMP_KEY] == ts
        end

        assert_not_equal(nil, index, "#{ns}@#{ts} not found.")
      end

      assert_equal(CONFIG_DATA.size, count)
    end
  end
end

