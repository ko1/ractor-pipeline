# frozen_string_literal: true

# Collects the numbers quoted in README (min of 3 runs each) and prints
# them as a markdown table.
#
#   ruby -I lib examples/readme_bench.rb

Warning[:experimental] = false

require "ractor/pipeline"
require "json"

include Ractor::Pipeline

LANES = [2, 4, 8, 16].freeze

def fib(n) = n < 2 ? n : fib(n - 1) + fib(n - 2)
def busy(n) = (i = 0; i += 1 while i < n; i)

def best(times = (ENV["BEST"] || 5).to_i)
  times.times.map { t = Time.now; yield; Time.now - t }.min
end

MW, MH, MITER = 78, 48, 3000

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

def agg(lines)
  st = {}
  lines.each do |l|
    r = JSON.parse(l)
    s = (st[r["path"]] ||= [0, 0, 0.0])
    s[0] += 1
    s[1] += 1 if r["status"] >= 500
    s[2] += r["ms"]
  end
  st
end

puts RUBY_DESCRIPTION

rows = []

# 1. CPU-bound: 32 x fib(28)
items = 32
expected = (1..items).sum{ fib(28) }
base = best{ raise unless (1..items).sum{ fib(28) } == expected }
speedups = LANES.map do |n|
  dt = best{ raise unless stream(1..items).pipe(lanes: n){ fib(28) }.reduce(0){ |a, v| a + v } == expected }
  base / dt
end
rows << ["fib(28) x 32 (uniform)", base, speedups]

# 2. skewed load: 1 heavy + 31 light
jobs = [40_000_000] + [4_000_000] * 31
base = best{ jobs.each{ busy(it) } }
speedups = LANES.map do |n|
  dt = best{ stream(jobs).pipe(lanes: n){ busy(it) }.join }
  base / dt
end
rows << ["skewed load (1 heavy + 31 light)", base, speedups]

# 3. mandelbrot rows (uneven cost, ordered reassembly)
base = best{ (0...MH).map{ mandel_row(it) } }
speedups = LANES.map do |n|
  dt = best{ stream(0...MH).pipe(lanes: n){ [it, mandel_row(it)] }.reduce(Array.new(MH)){ |a, (y, r)| a[y] = r; a } }
  base / dt
end
rows << ["mandelbrot (48 rows, uneven)", base, speedups]

# 4. JSONL aggregation (allocation-bound), chunk-aggregate style
rng = Random.new(42)
paths = ["/", "/api/users", "/api/items", "/api/search", "/login", "/assets/app.js"]
lines = Array.new(400_000) do
  %({"path":"#{paths[rng.rand(paths.size)]}","status":#{rng.rand(100) < 3 ? 500 : 200},"ms":#{(rng.rand * 300).round(1)}})
end
chunks = lines.each_slice(1000).to_a
base = best{ chunks.each{ agg(it) } }
speedups = LANES.map do |n|
  dt = best do
    stream(chunks).
      pipe(lanes: n){ agg(it) }.
      reduce({}){ |a, st| st.each{ |k, (q, e, m)| x = (a[k] ||= [0, 0, 0.0]); x[0] += q; x[1] += e; x[2] += m }; a }
  end
  base / dt
end
rows << ["JSONL aggregation (400k lines, alloc-bound)", base, speedups]

puts
puts "| workload (serial time) | " + LANES.map{ "lanes #{it}" }.join(" | ") + " |"
puts "|---|" + LANES.map{ "---" }.join("|") + "|"
rows.each do |name, base, speedups|
  puts "| #{name} (#{"%.2fs" % base}) | " + speedups.map{ "x%.2f" % it }.join(" | ") + " |"
end

# 5. granularity (per-line vs batch)
gl = lines.first(100_000)
base = best{ gl.count{ it.include?(%("status":500)) } }
per_line = best{ stream(gl).filter_pipe(lanes: 4){ it.include?(%("status":500)) }.count }
batched  = best{ stream(gl, batch: 1000).filter_pipe(lanes: 4){ it.include?(%("status":500)) }.count }
puts
printf "granularity: serial %.4fs / per-line %.3fs / batch1000 %.4fs\n", base, per_line, batched

# 6. throttling
class CountingSource
  attr_reader :n

  def initialize = @n = 0
  def each = loop{ yield (@n += 1) }
end
cs = CountingSource.new
stream(cs).pipe(lanes: 2){ it }.first(3)
printf "throttle: source reads = %d (first(3), infinite source)\n", cs.n
