# frozen_string_literal: true

# Feature tour of Ractor::Pipeline. Self-contained: file-based samples read
# this file itself.
#
#   ruby -I lib examples/demo.rb

Warning[:experimental] = false # suppress "Ractor API is experimental"

require "ractor/pipeline"

include Ractor::Pipeline

def section(title)
  puts
  puts "== #{title}"
  yield
end

section "basic: pipe + reduce" do
  sum = stream([1, 2, 3]).
          pipe{ it * 2 }.
          reduce(0){ |acc, n| acc + n }
  p sum #=> 12
end

section "filter_pipe + pipe + to_a (lanes: 1 keeps order)" do
  result = stream(1..10).
             filter_pipe{ it.even? }.
             pipe{ it * 10 }.
             to_a
  p result #=> [20, 40, 60, 80, 100]
end

section "wc: cat #{File.basename(__FILE__)} | grep Ractor | wc" do
  lines, words, bytes =
    stream(File.foreach(__FILE__)).
      filter_pipe(lanes: 4){ it.include?("Ractor") }.
      reduce([0, 0, 0]) do |(l, w, b), line|
        [l + 1, w + line.scan(/\S+/).size, b + line.bytesize]
      end
  puts "lines: #{lines}, words: #{words}, bytes: #{bytes}"
end

section "pipeline parallelism: two stages overlap" do
  n = 6
  wait = 0.05

  t = Time.now
  stream(1..n).
    pipe{ sleep(0.05); it }.     # stage 1
    pipe{ sleep(0.05); it }.     # stage 2 works while stage 1 handles the next element
    each{}
  pipeline_time = Time.now - t

  printf "naive:    %.3fs (= n * 2 stages * wait)\n", n * 2 * wait
  printf "pipeline: %.3fs\n", pipeline_time
end

section "tee: broadcast to branches, merged output (unordered)" do
  result = stream(1..5).
             tee(
               pipe{ [:double, it * 2] },
               pipe{ [:square, it * it] },
             ).
             to_a
  p result.sort_by{ |tag, v| [tag.to_s, v] }
end

section "tee with filter branches" do
  evens, odds = stream(1..10).
                  tee(
                    filter_pipe{ it.even? }.pipe{ [:even, it] },
                    filter_pipe{ it.odd?  }.pipe{ [:odd,  it] },
                  ).
                  reduce([[], []]) do |(evens, odds), (tag, n)|
                    tag == :even ? [evens << n, odds] : [evens, odds << n]
                  end
  p evens: evens.sort, odds: odds.sort
end

section "flat_pipe: 1 input -> N outputs" do
  count = stream([__FILE__] * 3).          # the same file, three times
            flat_pipe{ File.foreach(it) }.  # 1 file -> N lines
            filter_pipe(lanes: 2){ it.include?("section") }.
            count
  puts "lines containing 'section' (x3): #{count}"
end

section "batch: transparent batching (blocks still see single elements)" do
  result = stream(1..10, batch: 3).
             filter_pipe{ it.even? }.
             pipe{ it * 10 }.
             to_a
  p result #=> [20, 40, 60, 80, 100] (batch 越しでも lanes: 1 は順序保持)
end

section "throttling: a multi-lane head reads the source on demand" do
  reads = 0
  counting = Enumerator.new do |y|
    loop { y << (reads += 1) }
  end
  stream(counting).pipe(lanes: 2){ it }.first(3)
  puts "source reads: #{reads} (push だと数十万読む)"
end

section "first: early termination of an infinite stream" do
  result = stream(1..).
             pipe{ it * it }.
             first(5)
  p result #=> [1, 4, 9, 16, 25]
end

section "exception: raised in a stage, re-raised at the terminal" do
  begin
    stream(1..10).
      pipe{ raise "boom at #{it}" if it == 5; it }.
      to_a
  rescue RuntimeError => e
    puts "caught: #{e.message} (#{e.class})"
  end
end

section "block isolation: outer variables are snapshotted at stage construction" do
  factor = 10
  p stream(1..3).pipe{ it * factor }.to_a #=> [10, 20, 30]

  # Referencing a non-shareable value fails early, at .pipe time.
  buf = String.new("mutable")
  begin
    stream(1..3).pipe{ buf + it.to_s }
  rescue Ractor::IsolationError => e
    puts "caught: #{e.message}"
  end
end

puts
puts "done."
