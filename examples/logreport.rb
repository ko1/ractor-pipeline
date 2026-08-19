# frozen_string_literal: true

# A sample application: access-log report generator.
#
#   ruby -I lib examples/logreport.rb [logfile]
#
# Reads a JSONL access log from disk (a synthetic ~27MB / 400k-line file
# is generated on first run), aggregates per-path stats, an hourly
# histogram, status classes and the slowest endpoints, and prints a
# report. Both a serial and a pipeline implementation are run and timed
# end to end, including file I/O.
#
# Pipeline shape: the feeder thread reads the file in 2000-line chunks
# (disk I/O overlaps with parsing), 8 lanes parse and pre-aggregate their
# chunks, and the caller merges the small per-chunk aggregates.

Warning[:experimental] = false

require "ractor/pipeline"
require "json"

include Ractor::Pipeline

LINES = 400_000
CHUNK = 2_000
LANES = 8

LOGFILE = ARGV[0] || File.join(__dir__, "..", "tmp", "access.jsonl")

def generate_log(path)
  require "fileutils"
  FileUtils.mkdir_p(File.dirname(path))
  rng = Random.new(42)
  paths = ["/", "/api/users", "/api/items", "/api/search", "/api/orders",
           "/login", "/logout", "/assets/app.js", "/assets/app.css", "/healthz"]
  File.open(path, "w") do |f|
    LINES.times do
      pa = paths[rng.rand(paths.size)]
      hour = rng.rand(24)
      status = rng.rand(100) < 3 ? [500, 502, 503].sample(random: rng) : (rng.rand(100) < 8 ? 404 : 200)
      ms = ((pa.start_with?("/api") ? 40 : 5) + rng.rand * 200).round(1)
      bytes = 200 + rng.rand(5_000)
      f.puts %({"hour":#{hour},"path":"#{pa}","status":#{status},"ms":#{ms},"bytes":#{bytes}})
    end
  end
end

# NOTE: a method, not a constant holding a lambda -- a Proc in a constant
# is non-shareable and worker Ractors cannot read it.
def empty_agg = { paths: {}, hours: Array.new(24, 0), classes: Hash.new(0) }

# lines (Array of JSONL strings) -> small aggregate
def aggregate(lines)
  agg = empty_agg
  lines.each do |line|
    rec = JSON.parse(line)
    st = (agg[:paths][rec["path"]] ||= [0, 0, 0.0, 0]) # req, err, ms, bytes
    st[0] += 1
    st[1] += 1 if rec["status"] >= 500
    st[2] += rec["ms"]
    st[3] += rec["bytes"]
    agg[:hours][rec["hour"]] += 1
    agg[:classes][rec["status"] / 100] += 1
  end
  agg
end

def merge(a, b)
  b[:paths].each do |k, (req, err, ms, by)|
    st = (a[:paths][k] ||= [0, 0, 0.0, 0])
    st[0] += req
    st[1] += err
    st[2] += ms
    st[3] += by
  end
  24.times { |h| a[:hours][h] += b[:hours][h] }
  b[:classes].each { |k, v| a[:classes][k] += v }
  a
end

def serial_run(path)
  File.foreach(path).each_slice(CHUNK).reduce(empty_agg) { |acc, ls| merge(acc, aggregate(ls)) }
end

def pipeline_run(path)
  stream(File.foreach(path).each_slice(CHUNK)).
    pipe(lanes: LANES){ aggregate(it) }.
    reduce(empty_agg){ |acc, agg| merge(acc, agg) }
end

def canon(agg)
  [agg[:paths].transform_values { |req, err, ms, by| [req, err, ms.round(2), by] }.sort,
   agg[:hours], agg[:classes].sort]
end

def report(agg, io = $stdout)
  total = agg[:classes].values.sum
  io.puts "requests: #{total}   " + agg[:classes].sort.map { |c, n| "#{c}xx: #{n}" }.join("  ")
  io.puts
  io.puts "top paths (by requests):"
  agg[:paths].sort_by { |_, st| -st[0] }.first(5).each do |pa, (req, err, ms, by)|
    io.printf "  %-16s %7d req %5d err %7.1f ms avg %6.1f MB\n", pa, req, err, ms / req, by / 1e6
  end
  io.puts
  io.puts "slowest endpoints (avg ms):"
  agg[:paths].sort_by { |_, st| -(st[2] / st[0]) }.first(3).each do |pa, (req, _, ms, _)|
    io.printf "  %-16s %7.1f ms avg (%d req)\n", pa, ms / req, req
  end
  io.puts
  peak = agg[:hours].each_with_index.max_by(&:first)
  io.puts "hourly peak: #{peak[1]}:00 (#{peak[0]} req)"
end

unless File.exist?(LOGFILE)
  puts "generating #{LINES} lines into #{LOGFILE} ..."
  generate_log(LOGFILE)
end
puts "log: #{LOGFILE} (#{(File.size(LOGFILE) / 1e6).round(1)} MB)"
puts

t = Time.now
serial = serial_run(LOGFILE)
serial_dt = Time.now - t
printf "serial   : %.3fs\n", serial_dt

t = Time.now
parallel = pipeline_run(LOGFILE)
parallel_dt = Time.now - t
printf "pipeline : %.3fs  (x%.2f, %d-line chunks, lanes: %d)\n",
       parallel_dt, serial_dt / parallel_dt, CHUNK, LANES

raise "mismatch" unless canon(serial) == canon(parallel)

puts
report(parallel)
