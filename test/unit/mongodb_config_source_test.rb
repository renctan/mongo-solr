require_relative "../test_helper"
require "#{PROJ_SRC_PATH}/mongodb_config_source"

class MockCursor
  def initialize(results)
    @results = results
    @count = 0
  end

  def next_document
    @count = @count + 1

    if @count > @results
      return nil
    else
      return @count
    end
  end

  def has_next
    return (@count >= @results)
  end
end

class MongoDBConfigSourceTest < Test::Unit::TestCase
  should "iterate all of the cursor results" do
    cursor_result_count = 2
    cursor = MockCursor.new(cursor_result_count)

    collection = mock()
    collection.stubs(:find).returns(cursor)

    config = MongoSolr::MongoDBConfigSource.new(collection)
    count = 0

    config.each do |doc|
      assert(doc.is_a? MongoSolr::ConfigFormatReader)
      count = count + 1
    end

    assert_equal(2, count)
  end
end

