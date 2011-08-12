require File.expand_path("../../test_helper", __FILE__)
require_relative "../config_db_fixture"
require "#{PROJ_SRC_PATH}/shard_config_format_reader"

class ShardConfigFormatReaderTest < Test::Unit::TestCase
  include MongoSolr

  context "basic test" do
    setup do
      @config_data = ConfigDBFixture::SHARD_CONFIG1
      @shard_id = ConfigDBFixture::SHARD_1
      @reader = MongoSolr::ShardConfigFormatReader.new(@shard_id, @config_data)
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
            x[SolrConfigConst::UPDATE_TIMESTAMP_KEY][@shard_id] == ts
        end

        assert_not_equal(nil, index, "#{ns}@#{ts} not found.")
      end

      assert_equal(@config_data.size, count)
    end
  end

  context "partial timestamp" do
    setup do
      @config_data = ConfigDBFixture::SHARD_CONFIG4
      @shard_id = ConfigDBFixture::SHARD_1
      @reader = MongoSolr::ShardConfigFormatReader.new(@shard_id, @config_data)
    end

    should "not extract checkpoint data if no timestamp for own shard" do
      data = @reader.get_checkpoint_data

      assert_equal(@config_data.first[SolrConfigConst::COMMIT_TIMESTAMP_KEY],
                   data.commit_ts)
      assert(data.empty?)
    end

    should "extract checkpoint data if timestamp for own shard exists" do
      shard_id = ConfigDBFixture::SHARD_2
      reader = MongoSolr::ShardConfigFormatReader.new(shard_id,
                                                      @config_data)
      data = reader.get_checkpoint_data

      assert_equal(@config_data.first[SolrConfigConst::COMMIT_TIMESTAMP_KEY],
                   data.commit_ts)

      count = 0
      data.each do |ns, ts|
        count += 1

        index = @config_data.index do |x|
          x[SolrConfigConst::NS_KEY] == ns and
            x[SolrConfigConst::UPDATE_TIMESTAMP_KEY][shard_id] == ts
        end

        assert_not_equal(nil, index, "#{ns}@#{ts} not found.")
      end

      assert_equal(@config_data.size, count)      
    end    
  end
end

