require File.expand_path("../../test_helper", __FILE__)
require_relative "../config_db_fixture"
require "#{PROJ_SRC_PATH}/shard_config_format_reader"

class ShardConfigFormatReaderTest < Test::Unit::TestCase
  include MongoSolr

  CONFIG_DATA = ConfigDBFixture::SHARD_CONFIG1
  SHARD_ID = ConfigDBFixture::SHARD_1

  context "basic test" do
    setup do
      @reader = MongoSolr::ShardConfigFormatReader.new(SHARD_ID, CONFIG_DATA)
    end

    should "extract solr location correctly" do
      assert_equal(CONFIG_DATA.first[SolrConfigConst::SOLR_URL_KEY], @reader.solr_loc)
    end

    should "extract ns_set correctly" do
      ns_set = @reader.get_ns_set
      assert_equal(CONFIG_DATA.size, ns_set.size)

      CONFIG_DATA.each do |doc|
        ns = doc[SolrConfigConst::NS_KEY]
        assert(ns_set.include?(ns), "#{ns} not included in #{ns_set.inspect}")
      end
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
            x[SolrConfigConst::UPDATE_TIMESTAMP_KEY][SHARD_ID] == ts
        end

        assert_not_equal(nil, index, "#{ns}@#{ts} not found.")
      end

      assert_equal(CONFIG_DATA.size, count)
    end
  end
end

