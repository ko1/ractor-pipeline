# frozen_string_literal: true

# Compare Ractor::Pipeline with the parallel gem (process/thread based)
# on the map-shaped workloads from README.
#
#   gem install parallel
#   ruby -I lib examples/vs_parallel.rb
#
# Note the models differ: Parallel.map is a data-parallel map over a
# ready-made collection (fork/IPC per job set); Ractor::Pipeline is an
# in-process streaming topology. This compares only the overlap.

Warning[:experimental] = false

require "ractor/pipeline"
require "json"
begin
  require "parallel"
rescue LoadError
  abort "parallel gem not installed (gem install parallel)"
end

include Ractor::Pipeline

def fib(n) = n < 2 ? n : fib(n - 1) + fib(n - 2)
def busy(n) = (i = 0; i += 1 while i < n; i)

def best(times = 3)
  times.times.map { t = Time.now; yield; Time.now - t }.min
end

def row(label, base, dt)
  printf "  %-32s %8.3fs  (x%.2f)\n", label, dt, base / dt
end

puts RUBY_DESCRIPTION
puts "parallel #{Parallel::VERSION}"

section = ->(title){ puts; puts "== #{title}" }

# -- 1. uniform CPU: 32 x fib(28) ----------------------------------------
section.call "uniform CPU: 32 x fib(28)"
items = (1..32).to_a
expected = items.sum{ fib(28) }
base = best{ raise unless items.sum{ fib(28) } == expected }
printf "  %-32s %8.3fs\n", "serial", base
[8, 16].each do |n|
  dt = best{ raise unless stream(items).pipe(lanes: n){ fib(28) }.reduce(0){ |a, v| a + v } == expected }
  row "Ractor::Pipeline lanes: #{n}", base, dt
end
[8, 16].each do |n|
  dt = best{ raise unless Parallel.map(items, in_processes: n){ fib(28) }.sum == expected }
  row "Parallel in_processes: #{n}", base, dt
end
dt = best{ raise unless Parallel.map(items, in_threads: 8){ fib(28) }.sum == expected }
row "Parallel in_threads: 8 (GVL)", base, dt

# -- 2. skewed load: 1 heavy + 31 light ----------------------------------
section.call "skewed load: 1 heavy (~0.3s) + 31 light (~30ms)"
jobs = [40_000_000] + [4_000_000] * 31
base = best{ jobs.each{ busy(it) } }
printf "  %-32s %8.3fs\n", "serial", base
dt = best{ stream(jobs).pipe(lanes: 4){ busy(it) }.join }
row "Ractor::Pipeline lanes: 4", base, dt
dt = best{ Parallel.each(jobs, in_processes: 4){ busy(it) } }
row "Parallel in_processes: 4", base, dt

# -- 3. JSONL aggregation (allocation-bound) ------------------------------
section.call "JSONL aggregation: 400k lines, 1000-line chunks"
rng = Random.new(42)
paths = ["/", "/api/users", "/api/items", "/api/search", "/login", "/assets/app.js"]
lines = Array.new(400_000) do
  %({"path":"#{paths[rng.rand(paths.size)]}","status":#{rng.rand(100) < 3 ? 500 : 200},"ms":#{(rng.rand * 300).round(1)}})
end
chunks = lines.each_slice(1000).to_a

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

def merge_all(stats_list)
  stats_list.each_with_object({}) do |st, a|
    st.each{ |k, (q, e, m)| x = (a[k] ||= [0, 0, 0.0]); x[0] += q; x[1] += e; x[2] += m }
  end
end

base = best{ merge_all(chunks.map{ agg(it) }) }
printf "  %-32s %8.3fs\n", "serial", base
dt = best do
  stream(chunks).
    pipe(lanes: 8){ agg(it) }.
    reduce({}){ |a, st| st.each{ |k, (q, e, m)| x = (a[k] ||= [0, 0, 0.0]); x[0] += q; x[1] += e; x[2] += m }; a }
end
row "Ractor::Pipeline lanes: 8", base, dt
dt = best{ merge_all(Parallel.map(chunks, in_processes: 8){ agg(it) }) }
row "Parallel in_processes: 8", base, dt
dt = best{ merge_all(Parallel.map(chunks, in_threads: 8){ agg(it) }) }
row "Parallel in_threads: 8 (GVL)", base, dt

# -- 4. per-job overhead: tiny jobs ---------------------------------------
section.call "per-job overhead: 10k trivial jobs (it + 1)"
tiny = (1..10_000).to_a
base = best{ tiny.sum{ it + 1 } }
printf "  %-32s %8.3fs\n", "serial", base
dt = best{ raise unless stream(tiny, batch: 500).pipe(lanes: 4){ it + 1 }.count == 10_000 }
row "Ractor::Pipeline lanes:4 batch:500", base, dt
dt = best{ raise unless Parallel.map(tiny, in_processes: 4){ it + 1 }.size == 10_000 }
row "Parallel in_processes: 4", base, dt
