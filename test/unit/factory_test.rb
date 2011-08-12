require File.expand_path("../../test_helper", __FILE__)
require "#{PROJ_SRC_PATH}/factory"

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

class FactoryTest < Test::Unit::TestCase
  include MongoSolr

  should "partially apply arguments" do
    factory = Factory.new(DummyClass, 1)
    obj = factory.create(2, 3, 4)

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal(3, obj.c)
    assert_equal(4, obj.d)
  end

  should "return type correctly" do
    factory = Factory.new(DummyClass, 1)
    assert_equal(DummyClass, factory.type)
  end

  should "chain factories" do
    factory = Factory.new(DummyClass, 1)
    factory2 = Factory.new(factory, 2)
    factory3 = Factory.new(factory2, 3)

    obj = factory3.create(4)

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal(3, obj.c)
    assert_equal(4, obj.d)
  end

  should "return type correctly after chaining" do
    factory = Factory.new(DummyClass, 1)
    factory2 = Factory.new(factory, 2)

    assert_equal(DummyClass, factory2.type)
  end

  should "properly pass blocks" do
    dummy = mock()
    dummy.expects(:call_me).once

    factory = Factory.new(DummyClass, 1, 2, 3)
    obj = factory.create(4) { dummy.call_me }

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal(3, obj.c)
    assert_equal(4, obj.d)
  end

  should "properly pass arrays" do
    factory = Factory.new(DummyClass, [1, 2], 3)
    obj = factory.create(4, [5, 6])

    assert_equal([1, 2], obj.a)
    assert_equal(3, obj.b)
    assert_equal(4, obj.c)
    assert_equal([5, 6], obj.d)
  end

  should "properly pass zero args to n args" do
    factory = Factory.new(DummyClass)
    obj = factory.create(1, "e", "f", 6)

    assert_equal(1, obj.a)
    assert_equal("e", obj.b)
    assert_equal("f", obj.c)
    assert_equal(6, obj.d)
  end

  should "properly handle complete args" do
    factory = Factory.new(DummyClass, 1, 2, 3, 4)
    obj = factory.create

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal(3, obj.c)
    assert_equal(4, obj.d)
  end

  should "properly handle no args classes" do
    factory = Factory.new(ZeroArgs)
    obj = factory.create

    assert_equal(ZeroArgs::ONLY_VAL, obj.val)
  end

  should "properly handle one arg classes with partial application (non-array)" do
    val = "hilfe!"
    factory = Factory.new(OneArg, val)
    obj = factory.create

    assert_equal(val, obj.val)
  end

  should "properly handle one arg classes with partial application (array)" do
    array = [1, 2, 3]
    factory = Factory.new(OneArg, array)
    obj = factory.create

    assert_equal(array, obj.val)
  end

  should "properly handle one arg classes without partial application (non-array)" do
    val = "hilfe!"
    factory = Factory.new(OneArg)
    obj = factory.create val

    assert_equal(val, obj.val)
  end

  should "properly handle one arg classes without partial application (array)" do
    array = [1, 2, 3]
    factory = Factory.new(OneArg)
    obj = factory.create array

    assert_equal(array, obj.val)
  end

  should "maintain the original partially applied arguments at the constructor" do
    factory = Factory.new(DummyClass, 1, 2, 3)
    obj = factory.create(4)

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal(3, obj.c)
    assert_equal(4, obj.d)

    obj = factory.create("x")

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal(3, obj.c)
    assert_equal("x", obj.d)
  end

  should "properly chain with no partially applied arguments for 1 arg constructors" do
    factory = Factory.new(OneArg)
    factory2 = Factory.new(factory)

    obj = factory.create([1, 2])
    assert_equal([1, 2], obj.val)
  end

  should "maintain the original partially applied arguments with chaining" do
    factory = Factory.new(DummyClass, 1, 2)
    factory2 = Factory.new(factory, 3)
    factory3 = Factory.new(factory, "x")

    obj = factory.create("one", "two")

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal("one", obj.c)
    assert_equal("two", obj.d)

    obj = factory2.create(4)

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal(3, obj.c)
    assert_equal(4, obj.d)

    obj = factory3.create("y")

    assert_equal(1, obj.a)
    assert_equal(2, obj.b)
    assert_equal("x", obj.c)
    assert_equal("y", obj.d)
  end
end

