require File.expand_path("../../test_helper", __FILE__)
require "#{PROJ_SRC_PATH}/factory"

class DummyClass
  attr_reader :a, :b, :c, :d

  def initialize(a, b, c, d)
    @a, @b, @c, @d = a, b, c, d
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
    factory3 = Factory.new(factory, 3)

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
end

