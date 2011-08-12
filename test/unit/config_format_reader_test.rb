require File.expand_path("../../test_helper", __FILE__)
require_relative "../config_db_fixture"
require "#{PROJ_SRC_PATH}/config_format_reader"

class ConfigFormatReaderTest < Test::Unit::TestCase
  include MongoSolr

  context "basic test" do
    setup do
      @config_data = ConfigDBFixture::CONFIG1
      @reader = MongoSolr::ConfigFormatReader.new(@config_data)
    end

    should "extract solr location correctly" do
      assert_equal(@config_data.first[SolrConfigConst::SOLR_URL_KEY], @reader.solr_loc)
    end

    should "extract ns_set correctly" do
      ns_set = @reader.get_ns_set
      assert_equal(@config_data.size, ns_set.size)

      @config_data.each do |doc|
        ns = doc[SolrConfigConst::NS_KEY]
        assert(ns_set.include?(ns), "#{ns} not included in #{ns_set.inspect}")
      end
    end

    should "extract checkpoint data correctly" do
      data = @reader.get_checkpoint_data

      assert_equal(@config_data.first[SolrConfigConst::COMMIT_TIMESTAMP_KEY],
                   data.commit_ts)

      count = 0
      data.each do |ns, ts|
        count += 1

        index = @config_data.index do |x|
          x[SolrConfigConst::NS_KEY] == ns and
            x[SolrConfigConst::UPDATE_TIMESTAMP_KEY] == ts
        end

        assert_not_equal(nil, index, "#{ns}@#{ts} not found.")
      end

      assert_equal(@config_data.size, count)
    end
  end

  context "no timestamp" do
    setup do
      @config_data = ConfigDBFixture::CONFIG4
      @reader = MongoSolr::ConfigFormatReader.new(@config_data)
    end

    should "not extract checkpoint data" do
      data = @reader.get_checkpoint_data

      assert_equal(@config_data.first[SolrConfigConst::COMMIT_TIMESTAMP_KEY],
                   data.commit_ts)
      assert(data.empty?)
    end
  end
end

