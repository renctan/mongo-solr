require File.expand_path("../../test_helper", __FILE__)
require "#{PROJ_SRC_PATH}/object_builder"

class DummyClass
  attr_reader :a, :b, :c, :d

  def initialize(a, b, c, d)
    @a = a
    @b = b
    @c = c
    @d = d

    yield if block_given?
  end
end

class ZeroArgs
  ONLY_VAL = "dummy"

  attr_reader :val

  def initialize
    @val = ONLY_VAL
  end
end

class OneArg
  attr_reader :val

  def initialize(val)
    @val = val
  end
end

class ObjectBuilderTest < Test::Unit::TestCase
  include MongoSolr

  should "partially apply arguments" do
    builder = ObjectBuilder.new(DummyClass, 1)
    obj = builder.create(2, 3, 4)

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal(3, obj.c)
    assert_equal(4, obj.d)
  end

  should "return type correctly" do
    builder = ObjectBuilder.new(DummyClass, 1)
    assert_equal(DummyClass, builder.type)
  end

  should "chain factories" do
    builder = ObjectBuilder.new(DummyClass, 1)
    builder2 = ObjectBuilder.new(builder, 2)
    builder3 = ObjectBuilder.new(builder2, 3)

    obj = builder3.create(4)

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal(3, obj.c)
    assert_equal(4, obj.d)
  end

  should "return type correctly after chaining" do
    builder = ObjectBuilder.new(DummyClass, 1)
    builder2 = ObjectBuilder.new(builder, 2)

    assert_equal(DummyClass, builder2.type)
  end

  should "properly pass blocks" do
    dummy = mock()
    dummy.expects(:call_me).once

    builder = ObjectBuilder.new(DummyClass, 1, 2, 3)
    obj = builder.create(4) { dummy.call_me }

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal(3, obj.c)
    assert_equal(4, obj.d)
  end

  should "properly pass arrays" do
    builder = ObjectBuilder.new(DummyClass, [1, 2], 3)
    obj = builder.create(4, [5, 6])

    assert_equal([1, 2], obj.a)
    assert_equal(3, obj.b)
    assert_equal(4, obj.c)
    assert_equal([5, 6], obj.d)
  end

  should "properly pass zero args to n args" do
    builder = ObjectBuilder.new(DummyClass)
    obj = builder.create(1, "e", "f", 6)

    assert_equal(1, obj.a)
    assert_equal("e", obj.b)
    assert_equal("f", obj.c)
    assert_equal(6, obj.d)
  end

  should "properly handle complete args" do
    builder = ObjectBuilder.new(DummyClass, 1, 2, 3, 4)
    obj = builder.create

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal(3, obj.c)
    assert_equal(4, obj.d)
  end

  should "properly handle no args classes" do
    builder = ObjectBuilder.new(ZeroArgs)
    obj = builder.create

    assert_equal(ZeroArgs::ONLY_VAL, obj.val)
  end

  should "properly handle one arg classes with partial application (non-array)" do
    val = "hilfe!"
    builder = ObjectBuilder.new(OneArg, val)
    obj = builder.create

    assert_equal(val, obj.val)
  end

  should "properly handle one arg classes with partial application (array)" do
    array = [1, 2, 3]
    builder = ObjectBuilder.new(OneArg, array)
    obj = builder.create

    assert_equal(array, obj.val)
  end

  should "properly handle one arg classes without partial application (non-array)" do
    val = "hilfe!"
    builder = ObjectBuilder.new(OneArg)
    obj = builder.create val

    assert_equal(val, obj.val)
  end

  should "properly handle one arg classes without partial application (array)" do
    array = [1, 2, 3]
    builder = ObjectBuilder.new(OneArg)
    obj = builder.create array

    assert_equal(array, obj.val)
  end

  should "maintain the original partially applied arguments at the constructor" do
    builder = ObjectBuilder.new(DummyClass, 1, 2, 3)
    obj = builder.create(4)

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal(3, obj.c)
    assert_equal(4, obj.d)

    obj = builder.create("x")

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal(3, obj.c)
    assert_equal("x", obj.d)
  end

  should "properly chain with no partially applied arguments for 1 arg constructors" do
    builder = ObjectBuilder.new(OneArg)
    builder2 = ObjectBuilder.new(builder)

    obj = builder.create([1, 2])
    assert_equal([1, 2], obj.val)
  end

  should "maintain the original partially applied arguments with chaining" do
    builder = ObjectBuilder.new(DummyClass, 1, 2)
    builder2 = ObjectBuilder.new(builder, 3)
    builder3 = ObjectBuilder.new(builder, "x")

    obj = builder.create("one", "two")

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal("one", obj.c)
    assert_equal("two", obj.d)

    obj = builder2.create(4)

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal(3, obj.c)
    assert_equal(4, obj.d)

    obj = builder3.create("y")

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal("x", obj.c)
    assert_equal("y", obj.d)
  end
end

