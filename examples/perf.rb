# frozen_string_literal: true

# Performance demos of Ractor::Pipeline. Self-contained (synthetic data).
#
#   ruby -I lib examples/perf.rb
#
# Note: speedup ceilings depend heavily on the machine (P/E core mix,
# all-core clock drop, etc.), and allocation-heavy stage blocks scale worse
# than pure computation.

Warning[:experimental] = false # suppress "Ractor API is experimental"

require "ractor/pipeline"

include Ractor::Pipeline

def section(title)
  puts
  puts "== #{title}"
  yield
end

def bench
  t = Time.now
  result = yield
  [result, Time.now - t]
end

def report(label, dt, base = nil)
  if base
    printf "  %-36s %8.3fs  (x%.2f)\n", label, dt, base / dt
  else
    printf "  %-36s %8.3fs\n", label, dt
  end
end

def fib(n) = n < 2 ? n : fib(n - 1) + fib(n - 2)

section "CPU-bound scaling: 16 x fib(30), pipe(lanes: n)" do
  items = 16

  expected, base = bench{ (1..items).sum{ fib(30) } }
  report "serial", base

  [1, 2, 4, 8, 16].each do |n|
    result, dt = bench do
      stream(1..items)
        .pipe(lanes: n){ fib(30) }
        .reduce(0){ |acc, v| acc + v }
    end
    raise "mismatch" unless result == expected
    report "pipe(lanes: #{n})", dt, base
  end
end

# Synthetic corpus for the text-processing demos.
WORDS = %w[Ractor pipeline port stream lane stage worker copy share value].freeze

def make_corpus(docs, lines_per_doc)
  rng = Random.new(42)
  Array.new(docs) do
    Array.new(lines_per_doc) do
      Array.new(8){ WORDS[rng.rand(WORDS.size)] }.join(" ")
    end
  end
end

def word_tally(lines)
  tally = Hash.new(0)
  lines.each do |line|
    line.scan(/\w+/){ |w| tally[w] += 1 }
  end
  tally
end

section "map-reduce: word ranking, 1 document = 1 message" do
  corpus = make_corpus(160, 2_000)

  serial_top, base = bench do
    corpus.map{ word_tally(it) }
          .reduce(Hash.new(0)){ |acc, t| t.each{ |w, n| acc[w] += n }; acc }
          .max_by(3){ |_, n| n }
  end
  report "serial (#{corpus.size} docs)", base

  parallel_top, dt = bench do
    stream(corpus)
      .pipe(lanes: 8){ word_tally(it) }
      .reduce(Hash.new(0)){ |acc, t| t.each{ |w, n| acc[w] += n }; acc }
      .max_by(3){ |_, n| n }
  end
  report "pipe(lanes: 8) + reduce", dt, base

  raise "mismatch" unless serial_top == parallel_top
  puts "  top words: " + serial_top.map{ |w, n| "#{w}(#{n})" }.join(", ")
end

MW, MH, MITER = 78, 48, 3000

# Allocation-light row computation: allocation-heavy stage blocks scale
# worse across Ractors than pure computation.
def mandel_row(y)
  ci = -1.0 + 2.0 * y / MH
  row = String.new(capacity: MW)
  x = 0
  while x < MW
    cr = -2.2 + 3.2 * x / MW
    zr = zi = 0.0
    n = 0
    while n < MITER && zr * zr + zi * zi < 4.0
      zr, zi = zr * zr - zi * zi + cr, 2 * zr * zi + ci
      n += 1
    end
    row << (n == MITER ? "*" : " ")
    x += 1
  end
  row
end

section "mandelbrot: rows in parallel, reassembled by row index" do
  serial_pic, base = bench{ (0...MH).map{ mandel_row(it) } }
  report "serial (#{MW}x#{MH}, iter=#{MITER})", base

  # lanes: 8 is unordered, so carry the row index and reassemble.
  parallel_pic, dt = bench do
    stream(0...MH)
      .pipe(lanes: 8){ [it, mandel_row(it)] }
      .reduce(Array.new(MH)){ |acc, (y, row)| acc[y] = row; acc }
  end
  report "pipe(lanes: 8)", dt, base

  raise "mismatch" unless serial_pic == parallel_pic
  puts parallel_pic.map{ |row| "  |#{row}|" }
end

section "granularity: per-line vs per-chunk messages" do
  # Fine-grained messages (1 line = 1 message) are dominated by
  # copy/communication cost; chunking restores the speedup.
  lines = make_corpus(1, 20_000).first

  serial_count, base = bench{ lines.count{ it.include?("Ractor") } }
  report "serial (#{lines.size} lines)", base

  line_count, dt = bench do
    stream(lines)
      .filter_pipe(lanes: 4){ it.include?("Ractor") }
      .count
  end
  report "per-line filter_pipe(lanes: 4)", dt, base

  chunk_count, dt = bench do
    stream(lines.each_slice(1000))
      .pipe(lanes: 4){ it.count{ |line| line.include?("Ractor") } }
      .reduce(0){ |acc, n| acc + n }
  end
  report "1000-line chunks, pipe(lanes: 4)", dt, base

  raise "mismatch" unless serial_count == line_count && serial_count == chunk_count
  puts "  matching lines: #{serial_count}"
end

puts
puts "done."
