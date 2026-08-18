# frozen_string_literal: true

require "test_helper"

class Ractor::PipelineTest < Test::Unit::TestCase
  include Ractor::Pipeline

  test "VERSION" do
    assert do
      ::Ractor::Pipeline.const_defined?(:VERSION)
    end
  end

  test "pipe + reduce" do
    sum = stream([1, 2, 3]).
            pipe{ it * 2 }.
            reduce(0){ |acc, n| acc + n }
    assert_equal 12, sum
  end

  test "single-lane stages preserve order" do
    result = stream(1..10).
               pipe{ it * 10 }.
               pipe{ it + 1 }.
               to_a
    assert_equal((1..10).map{ it * 10 + 1 }, result)
  end

  test "filter_pipe sends the original element" do
    result = stream(1..10).
               filter_pipe{ it.even? }.
               to_a
    assert_equal [2, 4, 6, 8, 10], result
  end

  test "flat_pipe expands one input to many outputs" do
    result = stream([1, 2, 3]).
               flat_pipe{ [it] * it }.
               to_a
    assert_equal [1, 2, 2, 3, 3, 3], result.sort
  end

  test "multi-lane stage processes every element" do
    result = stream(1..100).
               pipe(lanes: 4){ it * 2 }.
               to_a
    assert_equal((1..100).map{ it * 2 }, result.sort)
  end

  test "empty pipeline just forwards the source" do
    assert_equal [1, 2, 3], stream([1, 2, 3]).to_a
  end

  test "stream1 flows one object as a single element" do
    assert_equal [42], stream1(42).to_a
    # even an each-able object is a single element
    assert_equal [[1, 2, 3]], stream1([1, 2, 3]).to_a
    assert_equal 6, stream1([1, 2, 3]).pipe{ it.sum }.first
  end

  test "count" do
    assert_equal 50, stream(1..100).filter_pipe(lanes: 2){ it.odd? }.count
  end

  test "each yields every element and returns self" do
    seen = []
    pl = stream(1..5).pipe{ it + 1 }
    assert_same pl, pl.each{ seen << it }
    assert_equal [2, 3, 4, 5, 6], seen
  end

  test "first terminates an infinite stream" do
    assert_equal [1, 4, 9], stream(1..).pipe{ it * it }.first(3)
    assert_equal 1, stream(1..).pipe{ it }.first
    assert_equal [], stream(1..).pipe{ it }.first(0)
  end

  test "tee broadcasts to every branch" do
    result = stream(1..3).
               tee(
                 pipe{ [:a, it] },
                 pipe{ [:b, it] },
               ).
               to_a
    assert_equal [[:a, 1], [:a, 2], [:a, 3], [:b, 1], [:b, 2], [:b, 3]],
                 result.sort_by{ |tag, n| [tag.to_s, n] }
  end

  test "stage exception is re-raised at the terminal operation" do
    e = assert_raise(RuntimeError) do
      stream(1..10).pipe{ raise "boom at #{it}" if it == 5; it }.to_a
    end
    assert_equal "boom at 5", e.message
  end

  test "source exception is re-raised at the terminal operation" do
    src = Object.new
    def src.each = raise IOError, "broken source"
    assert_raise(IOError) do
      stream(src).pipe{ it }.to_a
    end
  end

  test "capturing a non-shareable value raises at construction time" do
    buf = String.new("mutable")
    assert_raise(Ractor::IsolationError) do
      stream(1..3).pipe{ buf + it.to_s }
    end
  end

  test "capturing a shareable value snapshots it" do
    factor = 10
    assert_equal [10, 20, 30], stream(1..3).pipe{ it * factor }.to_a
  end

  test "argument validation" do
    assert_raise(ArgumentError){ stream(nil) }
    assert_raise(ArgumentError){ stream([]).pipe(lanes: 0){ it } }
    assert_raise(ArgumentError){ stream([]).pipe }
    assert_raise(ArgumentError){ stream([]).tee(pipe{ it }, Object.new) }
  end
end
