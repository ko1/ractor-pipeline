# frozen_string_literal: true

# A realistic-ish benchmark: aggregate statistics from JSONL access logs.
#
#   ruby -I lib examples/logstats.rb
#
# Each line is a JSON object like
#   {"path":"/api/users","status":200,"ms":12.3}
# and the task is: parse every line, count requests and errors per path,
# and sum latencies (i.e. the shape of a typical log-crunching script).
#
# The pipeline version chunks lines (1000 lines = 1 message), parses and
# pre-aggregates each chunk in parallel stage workers, and merges the
# small per-chunk hashes in the caller.

Warning[:experimental] = false # suppress "Ractor API is experimental"

require "ractor/pipeline"
require "json"

include Ractor::Pipeline

N_LINES = 400_000
CHUNK = 1_000
LANES = 8

def make_log(n)
  rng = Random.new(42)
  paths = ["/", "/api/users", "/api/items", "/api/search", "/login", "/assets/app.js"]
  Array.new(n) do
    path = paths[rng.rand(paths.size)]
    status = rng.rand(100) < 3 ? 500 : 200
    ms = (rng.rand * 300).round(1)
    %({"path":"#{path}","status":#{status},"ms":#{ms}})
  end
end

# chunk of lines -> small aggregate hash {path => [requests, errors, total_ms]}
# NOTE: no Hash.new{} here — a Hash with a default_proc cannot cross a
# Ractor boundary (Procs are not copyable).
def aggregate(lines)
  stats = {}
  lines.each do |line|
    rec = JSON.parse(line)
    s = (stats[rec["path"]] ||= [0, 0, 0.0])
    s[0] += 1
    s[1] += 1 if rec["status"] >= 500
    s[2] += rec["ms"]
  end
  stats
end

def merge(acc, stats)
  stats.each do |path, (req, err, ms)|
    a = (acc[path] ||= [0, 0, 0.0])
    a[0] += req
    a[1] += err
    a[2] += ms
  end
  acc
end

def report(label, stats, dt, base = nil)
  total = stats.values.sum(&:first)
  speedup = base ? "  (x%.2f)" % (base / dt) : ""
  printf "%-28s %8.3fs%s  (%d reqs, %d errors)\n",
         label, dt, speedup, total, stats.values.sum{ |_, e, _| e }
end

lines = make_log(N_LINES)
puts "#{N_LINES} JSONL lines, batch/chunk=#{CHUNK}, lanes=#{LANES}"

t = Time.now
serial = lines.each_slice(CHUNK).map{ aggregate(it) }.reduce({}){ |a, s| merge(a, s) }
serial_dt = Time.now - t
report "serial", serial, serial_dt

# Style 1: transparent batching. Blocks see single lines/records: parse in
# the stage workers and project each record down to a small tuple before
# it crosses the boundary (copying the whole parsed Hash would cost about
# as much as parsing it).
t = Time.now
per_record = stream(lines, batch: CHUNK).
               pipe(lanes: LANES){ r = JSON.parse(it); [r["path"], r["status"], r["ms"]] }.
               reduce({}) do |acc, (path, status, ms)|
                 s = (acc[path] ||= [0, 0, 0.0])
                 s[0] += 1
                 s[1] += 1 if status >= 500
                 s[2] += ms
                 acc
               end
per_record_dt = Time.now - t
report "batch + per-record reduce", per_record, per_record_dt, serial_dt

# Style 2: chunk-aggregate. One message carries a chunk of lines and the
# stage returns a small pre-aggregated hash, so the reduce merges
# #chunks hashes instead of touching every record.
t = Time.now
parallel = stream(lines.each_slice(CHUNK)).
             pipe(lanes: LANES){ aggregate(it) }.
             reduce({}){ |acc, stats| merge(acc, stats) }
parallel_dt = Time.now - t
report "chunk-aggregate + merge", parallel, parallel_dt, serial_dt

# Float sums depend on addition order (multi-lane stages are unordered),
# so compare with rounded latency totals.
def canon(stats) = stats.transform_values{ |req, err, ms| [req, err, ms.round(3)] }.sort
raise "mismatch" unless canon(serial) == canon(parallel) && canon(serial) == canon(per_record)

puts
puts "per-path stats:"
serial.sort_by{ |_, (req, _, _)| -req }.each do |path, (req, err, ms)|
  printf "  %-16s %7d reqs %5d errors %8.1f ms avg\n", path, req, err, ms / req
end
