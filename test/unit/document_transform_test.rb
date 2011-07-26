require File.expand_path("../../test_helper", __FILE__)
require "#{PROJ_SRC_PATH}/document_transform"

class DocumentTransformTest < Test::Unit::TestCase
  ARRAY_SEP = MongoSolr::DocumentTransform::DEFAULT_ARRAY_SEPARATOR
  HASH_SEP = MongoSolr::DocumentTransform::DEFAULT_HASH_SEPARATOR
  MY_ARRAY_SEP = "@"
  MY_HASH_SEP = "/"

  context "basic test" do
    should "make no changes on a flat doc" do
      test_doc = { :name => "foo", "num" => 123 }
      result = MongoSolr::DocumentTransform.translate_doc(test_doc)
      assert_equal(test_doc, result)
    end

    should "flatten a simple array" do
      test_doc = { :name => "foo", "bar" => %w[Hello World] }
      expected = { :name => "foo", "bar#{ARRAY_SEP}0" => "Hello", "bar#{ARRAY_SEP}1" => "World"}

      result = MongoSolr::DocumentTransform.translate_doc(test_doc)
      assert_equal(expected, result)
    end

    should "flatten a simple hash" do
      test_doc = { :name => "foo", "bar" => { "x" => 1, "y" => 2} }
      expected = { :name => "foo", "bar#{HASH_SEP}x" => 1, "bar#{HASH_SEP}y" => 2}

      result = MongoSolr::DocumentTransform.translate_doc(test_doc)
      assert_equal(expected, result)
    end

    should "flatten a simple BSON hash" do
      test_doc = BSON::OrderedHash.new()
      test_doc[:name] = "foo"

      inner_doc = BSON::OrderedHash.new()
      inner_doc["x"] = 1
      inner_doc["y"] = 2
      test_doc["bar"] = inner_doc

      expected = { :name => "foo", "bar#{HASH_SEP}x" => 1, "bar#{HASH_SEP}y" => 2}

      result = MongoSolr::DocumentTransform.translate_doc(test_doc)
      assert_equal(expected, result)
    end

    should "use custom array separator" do
      test_doc = { "foo" => [100, 200] }
      expected = { "foo#{MY_ARRAY_SEP}0" => 100, "foo#{MY_ARRAY_SEP}1" => 200 }

      result = MongoSolr::DocumentTransform.translate_doc(test_doc, MY_ARRAY_SEP, "_")
      assert_equal(expected, result)
    end

    should "use custom hash separator" do
      test_doc = { "foo" => { "bar" => "x" }}
      expected = { "foo#{MY_HASH_SEP}bar" => "x" }

      result = MongoSolr::DocumentTransform.translate_doc(test_doc, "_", MY_HASH_SEP)
      assert_equal(expected, result)
    end
  end

  context "nesting more than one level deep" do
    setup do
      @test_doc = BSON::OrderedHash.new()

      @inner_doc = BSON::OrderedHash.new()

      @inner_inner_doc = BSON::OrderedHash.new()
      @inner_inner_doc["x"] = 9
      @inner_inner_doc["y"] = 8
    end

    should "flatten array inside an array" do
      @test_doc["a"] = [[1, 2], ["end"]]
      expected = {
        "a#{MY_ARRAY_SEP}0#{MY_ARRAY_SEP}0" => 1,
        "a#{MY_ARRAY_SEP}0#{MY_ARRAY_SEP}1" => 2,
        "a#{MY_ARRAY_SEP}1#{MY_ARRAY_SEP}0" => "end",
      }

      result = MongoSolr::DocumentTransform.translate_doc(@test_doc, MY_ARRAY_SEP, HASH_SEP)
      assert_equal(expected, result)
    end

    should "flatten hash inside an array" do
      @inner_doc["bar"] = @inner_inner_doc
      @test_doc["a"] = ["foo", @inner_doc]

      expected = {
        "a#{MY_ARRAY_SEP}0" => "foo",
        "a#{MY_ARRAY_SEP}1#{HASH_SEP}bar#{HASH_SEP}x" => 9,
        "a#{MY_ARRAY_SEP}1#{HASH_SEP}bar#{HASH_SEP}y" => 8,
      }

      result = MongoSolr::DocumentTransform.translate_doc(@test_doc, MY_ARRAY_SEP, HASH_SEP)
      assert_equal(expected, result)
    end

    should "flatten hash inside a hash" do
      @inner_doc["foo"] = "oof"
      @inner_doc["bar"] = @inner_inner_doc
      @test_doc["h"] = @inner_doc

      expected = {
        "h#{HASH_SEP}foo" => "oof",
        "h#{HASH_SEP}bar#{HASH_SEP}x" => 9,
        "h#{HASH_SEP}bar#{HASH_SEP}y" => 8,
      }

      result = MongoSolr::DocumentTransform.translate_doc(@test_doc, MY_ARRAY_SEP, HASH_SEP)
      assert_equal(expected, result)
    end

    should "flatten array inside a hash" do
      @inner_doc["foo"] = "oof"
      @inner_doc["arr"] = [5, 4]
      @test_doc["h"] = @inner_doc

      expected = {
        "h#{HASH_SEP}foo" => "oof",
        "h#{HASH_SEP}arr#{MY_ARRAY_SEP}0" => 5,
        "h#{HASH_SEP}arr#{MY_ARRAY_SEP}1" => 4,
      }

      result = MongoSolr::DocumentTransform.translate_doc(@test_doc, MY_ARRAY_SEP, HASH_SEP)
      assert_equal(expected, result)
    end
  end

  context "mixed contents" do
    setup do
      @test_doc = BSON::OrderedHash.new()
      @inner_doc = BSON::OrderedHash.new()
      @inner_inner_doc = BSON::OrderedHash.new()
    end

    should "flatten all nested contents within an array" do
      @inner_doc["foo"] = "bar"
      @test_doc["test"] = [@inner_doc, [3, 4]]

      expected = {
        "test#{MY_ARRAY_SEP}0#{HASH_SEP}foo" => "bar",
        "test#{MY_ARRAY_SEP}1#{MY_ARRAY_SEP}0" => 3,
        "test#{MY_ARRAY_SEP}1#{MY_ARRAY_SEP}1" => 4,
      }

      result = MongoSolr::DocumentTransform.translate_doc(@test_doc, MY_ARRAY_SEP, HASH_SEP)
      assert_equal(expected, result)
    end

    should "flatten all nested contents within a hash" do
      @inner_inner_doc["deep"] = "blue"
      @inner_doc["h"] = @inner_inner_doc
      @inner_doc["a"] = [5, 6]
      @test_doc["test"] = @inner_doc

      expected = {
        "test#{HASH_SEP}h#{HASH_SEP}deep" => "blue",
        "test#{HASH_SEP}a#{MY_ARRAY_SEP}0" => 5,
        "test#{HASH_SEP}a#{MY_ARRAY_SEP}1" => 6,
      }

      result = MongoSolr::DocumentTransform.translate_doc(@test_doc, MY_ARRAY_SEP, HASH_SEP)
      assert_equal(expected, result)
    end
  end
end

