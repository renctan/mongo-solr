require File.expand_path("../../test_helper", __FILE__)
require "#{PROJ_SRC_PATH}/util"

class UtilTest < Test::Unit::TestCase
  INT_32_MAX = (2 << 31) - 1

  include MongoSolr::Util

  context "util" do
    should "properly convert bson timestamp to long" do
      ts = BSON::Timestamp.new(0x12345678, 0x90abcdef)
      assert_equal(0x1234567890abcdef, bsonts_to_long(ts))
    end

    should "properly convert long to bson timestamp" do
      assert_equal(BSON::Timestamp.new(0x90abcdef, 0x12345678),
                   long_to_bsonts(0x90abcdef12345678))
    end

    should "not lost in translation" do
      10.times do
        seconds = (INT_32_MAX * rand).to_i
        interval = (INT_32_MAX * rand).to_i
        ts = BSON::Timestamp.new(seconds, interval)

        trans_ts = long_to_bsonts(bsonts_to_long(ts))
        assert_equal(ts, trans_ts)
      end
    end
  end
end

