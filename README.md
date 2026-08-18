# Ractor::Pipeline

A DSL to build stream processing pipelines with Ractors. The basic model is
close to a Unix shell pipeline: `stream` produces an input stream, each
`pipe`/`filter_pipe` stage is a persistent Ractor connected by
`Ractor::Port`s, and terminal operations such as `reduce` consume the output
in the caller Ractor.

Unlike `enum.map{}.filter{}`, which describes only data operations, this DSL
intentionally couples data operations with the execution topology: what you
see in the code is the Ractor graph that runs.

```ruby
require "ractor/pipeline"
include Ractor::Pipeline

# conceptually: cat FILE | grep foo | wc
stream(File.foreach(name)).
  filter_pipe(lanes: 4){ it.include?("foo") }.
  reduce([0, 0, 0]) do |(lines, words, bytes), line|
    [lines + 1, words + line.scan(/\S+/).size, bytes + line.bytesize]
  end
```

```text
File.foreach ──> filter Ractor x4 ──> Port ──> reduce in caller
```

Key properties:

* **1 stage = `lanes:` persistent Ractors** — workers process many
  elements; no Ractor is created per element.
* **Demand-driven scheduling** — a multi-lane stage is fed by pull: a
  worker gets a new batch only when it asks for one, so a stuck worker
  never has work piling up behind it, and a fast or infinite source is
  read only as fast as the pipeline consumes it.
* **Transparent batching** — `stream(src, batch: 1000)` packs 1000
  elements per message to amortize the per-message cost; stage blocks
  still see single elements.

Requires Ruby 4.0+ (`Ractor::Port`, `Ractor.shareable_proc`). The Ractor API
is experimental, and so is this library.

## Overview

| DSL | Enumerable analogue | topology |
|---|---|---|
| `stream(src, batch: k)` | `src.each` | source (fed from the caller) |
| `stream1(obj)` | `[obj].each` | single-element source |
| `.pipe{ f(it) }` | `map` | 1 persistent Ractor |
| `.pipe(lanes: n){}` | `map` | n Ractors, demand-driven, unordered |
| `.filter_pipe{ pred(it) }` | `filter` | 1 Ractor (sends the original element) |
| `.flat_pipe{ enum }` | `flat_map` | 1 input -> N outputs |
| `.tee(pipe{}, pipe{})` | — | broadcast to branches, merged output |
| `.reduce(init){}` `.each{}` `.to_a` `.count` `.first(n)` | same | terminal, runs in the caller, no Ractor |

## API guide

`include Ractor::Pipeline` makes the vocabulary available
(`Ractor::Pipeline.stream(...)` also works). Stage-building methods return
the pipeline object, so they chain; nothing runs until a terminal operation
is called.

### `stream(source, batch: 1)`

Creates a pipeline whose input is each element of `source` (anything that
responds to `each`). Elements are fed from a background Thread in the
caller Ractor, concurrently with the terminal operation, so producing and
consuming overlap.

```ruby
stream([1, 2, 3])           # three elements
stream(1..)                 # infinite stream (terminate with .first etc.)
stream(File.foreach(name))  # one element per line
```

`batch: k` packs k elements into one message. This is transparent — stage
blocks still receive one element at a time — and it amortizes the
per-message cost (sync + copy + envelope) over k elements, which matters
whenever the work per element is small (see
[Measured performance](#measured-performance)). Batches shrink through
`filter_pipe` and are re-split to `<= k` after `flat_pipe`; the message
*count* is unchanged, so the amortization survives filtering.

When the first stage is multi-lane, the source is read on demand: the
feeder only reads ahead by the workers' open demand tokens, so
`stream(huge_or_infinite_source)` does not balloon memory.

`stream1(obj)` is a shorthand for `stream([obj])`: it flows `obj` as a
single element, even when `obj` itself is each-able.

```ruby
stream1(config).pipe{ build(it) }.first
```

### `pipe(lanes: 1){ block }`

A processing stage: applies the block to each element and sends the return
value downstream. The current element is `it`.

```ruby
stream([1, 2, 3]).pipe{ it * 2 }.to_a  #=> [2, 4, 6]
```

With `lanes: n`, the stage becomes n worker Ractors and the boundary into
it becomes demand-driven: each worker grants a small number of demand
tokens (2 per producer) and producers send a batch only to a worker they
hold a token for. A worker that is stuck on an expensive element simply
stops granting tokens, so the work is distributed by actual availability,
not round-robin. Completion order is not preserved (see
[Ordering](#ordering)).

```ruby
stream(rows).pipe(lanes: 8){ expensive(it) }
```

Consecutive stages form a pipeline and run concurrently — while stage 2
processes element k, stage 1 processes element k+1:

```ruby
stream(src).pipe{ parse(it) }.pipe{ format(it) }
#  src ──> Ractor(parse) ──> Ractor(format) ──> ...
```

Boundaries into a single consumer (a `lanes: 1` stage, or the terminal
operation) are plain push over the port's FIFO: no demand round-trip, no
reordering.

### `filter_pipe(lanes: 1){ block }`

Like `pipe`, but the block is a predicate: the *original element* (not the
block's return value) is sent downstream iff the block returns truthy.

```ruby
stream(1..10).filter_pipe{ it.even? }.to_a  #=> [2, 4, 6, 8, 10]
```

### `flat_pipe(lanes: 1){ block }`

The block returns an each-able object; each of its elements is sent
downstream individually (1 input -> N outputs). Useful for feeding many
files into one fixed-size Ractor graph:

```ruby
stream(file_names).
  flat_pipe{ File.foreach(it) }.           # 1 file -> N lines
  filter_pipe(lanes: 4){ it.include?("Ractor") }.
  count
```

### `tee(branch, branch, ...)`

Broadcasts each element to *every* branch (not load balancing). Branches
are receiver-less fragments; their outputs are merged, unordered, into one
downstream stream. Non-shareable elements are copied once per branch.

```ruby
evens, odds = stream(1..10).
                tee(
                  filter_pipe{ it.even? }.pipe{ [:even, it] },
                  filter_pipe{ it.odd?  }.pipe{ [:odd,  it] },
                ).
                reduce([[], []]) do |(evens, odds), (tag, n)|
                  tag == :even ? [evens << n, odds] : [evens, odds << n]
                end
```

### Terminal operations

Terminal operations run in the caller Ractor (no Ractor is created for
them) and start the pipeline. Their blocks are ordinary blocks — no
isolation restrictions, so a mutable accumulator is fine.

```ruby
pl.reduce(initial){ |acc, elem| ... }  # returns the final accumulator
pl.each{ |elem| ... }                  # yields each element, returns self
pl.to_a                                # collects into an Array
pl.count                               # number of output elements
pl.first                               # first element (stops the pipeline)
pl.first(n)                            # first n elements as an Array
```

`first` cancels the rest of the stream: it stops the feeder, closes its
output port, and broadcasts a cancel message, so workers drop their
backlog and terminate immediately (in-flight sends to closed ports raise
`Ractor::ClosedError` and cascade the shutdown upstream, like SIGPIPE in a
shell pipeline). Safe to use with an infinite `stream(1..)`.

### Errors

* An exception raised in a stage block cancels the pipeline and is
  re-raised by the terminal operation in the caller:

  ```ruby
  stream(1..10).pipe{ raise "boom" if it == 5; it }.to_a
  #=> RuntimeError "boom" raised from .to_a
  ```

* Stage blocks are isolated with `Ractor.shareable_proc` at construction
  time. Capturing a *shareable* outer value snapshots it; capturing a
  non-shareable value raises `Ractor::IsolationError` immediately at
  `.pipe` time (not at run time). `self` inside a stage block is `nil`,
  so top-level helper methods are callable but instance methods are not.

  ```ruby
  factor = 10
  stream(1..3).pipe{ it * factor }.to_a  #=> [10, 20, 30] (snapshot)

  buf = String.new
  stream(1..3).pipe{ buf << it.to_s }    #=> Ractor::IsolationError at .pipe
  ```

* Everything a stage block returns must be sendable through a
  `Ractor::Port`. A common gotcha: a Hash built with `Hash.new{ ... }`
  holds a Proc as its default and cannot cross the boundary — build plain
  hashes (`(h[k] ||= 0) += 1` style) in stage blocks.

* **Expected errors need no special vocabulary** — write plain `rescue`
  in the stage block. Batching is transparent, so a rescue is naturally
  per-element; unrescued exceptions keep the fail-fast behavior above.

  ```ruby
  pipe{ JSON.parse(it) rescue DEFAULT }                    # fallback
  pipe{ Integer(it) rescue raise(SKIP) }                   # drop bad elements
  pipe{ begin; fetch(it); rescue Timeout::Error; retry; end }
  ```

  Raising `SKIP` drops the current element. It is meant for exceptional
  cases — systematic thinning is what `filter_pipe` is for — and is
  priced accordingly: the happy path costs nothing, each skip costs one
  exception. `SKIP` inherits `Exception`, so a bare `rescue => e` in the
  block cannot swallow it; it also works from deep inside helper
  methods.

  To collect diagnostics, wire your own stderr: a `Ractor::Port` is
  shareable, so a stage block can capture one and report failures to the
  caller out-of-band.

  ```ruby
  errors = Ractor::Port.new
  result = stream(rows).
             pipe(lanes: 8){ parse(it) rescue (errors << it; raise SKIP) }.
             to_a
  ```

## Samples

### wc: `cat README.md | grep Ruby | wc`

```ruby
lines, words, bytes =
  stream(File.foreach("README.md")).
    filter_pipe(lanes: 4){ it.include?("Ruby") }.
    reduce([0, 0, 0]) do |(l, w, b), line|
      [l + 1, w + line.scan(/\S+/).size, b + line.bytesize]
    end
```

### Log crunching: parse JSONL, aggregate per path

One message carries a chunk of lines; each stage worker parses its chunk
and returns a small pre-aggregated hash, so the caller-side reduce merges
`#chunks` hashes instead of touching every record
(runnable version: [examples/logstats.rb](examples/logstats.rb)):

```ruby
stats = stream(lines.each_slice(1000)).
          pipe(lanes: 8){ aggregate(it) }.      # chunk -> {path => [req, err, ms]}
          reduce({}){ |acc, st| merge(acc, st) }
```

### Unordered results: carry an index and reassemble

```ruby
picture = stream(0...height).
            pipe(lanes: 8){ [it, render_row(it)] }.
            reduce(Array.new(height)){ |acc, (y, row)| acc[y] = row; acc }
```

### Early termination of an infinite stream

```ruby
stream(1..).pipe{ it * it }.first(5)  #=> [1, 4, 9, 16, 25]
```

### Fine-grained sources: use batch:

```ruby
stream(File.foreach(name), batch: 1000).
  filter_pipe(lanes: 4){ it.include?("VALUE") }.
  count
```

More runnable samples: [examples/demo.rb](examples/demo.rb) (feature tour),
[examples/perf.rb](examples/perf.rb) and
[examples/readme_bench.rb](examples/readme_bench.rb) (the measurements
below).

## Semantics notes

* <a name="ordering"></a>**Ordering**: a chain of `lanes: 1` stages
  preserves input order, with or without `batch:`. Multi-lane stages are
  unordered (elements overtake each other across lanes).
* **Boundaries copy**: non-shareable objects are deep-copied at each Ractor
  boundary (`Ractor::Port#send` default); shareable objects are passed by
  reference.
* **End of stream** propagates via an in-band EOS message: each worker
  counts EOS from its upstream producers, drains its buffered output, and
  broadcasts EOS downstream, so fan-in and fan-out shut down cleanly.
* **Cancellation** (`first`, exceptions) closes the terminal's port and
  broadcasts cancel; workers drop their backlog and exit, and the closure
  cascades upstream via `Ractor::ClosedError` (SIGPIPE-style).
* **Backpressure**: demand-driven boundaries are bounded (2 batches per
  producer/consumer pair), and a multi-lane head stage throttles the
  source itself. Push boundaries (into `lanes: 1` stages and the terminal)
  are unbounded FIFO queues.

## Measured performance

Numbers from `examples/readme_bench.rb` (min of 5 runs) on Ruby 4.1.0dev
(2026-08-18 master, 98b3b8034d) on a 20-thread laptop (Core i7-1370P,
6P+8E cores, WSL2). Absolute numbers vary a lot with machine, clocks, and
Ruby version — treat them as shape, not truth. Ruby 4.0.2 behaves
similarly except that multi-Ractor CPU throughput at high lane counts is
~25% lower.

**Lanes sweep** — speedup over serial for `pipe(lanes: n)`:

| workload (serial time) | lanes 2 | lanes 4 | lanes 8 | lanes 16 |
|---|---|---|---|---|
| fib(28) x 32, uniform CPU (0.52s) | x1.9 | x3.3 | x2.6 | x5.5 |
| skewed load, 1 heavy + 31 light (0.85s) | x1.8 | x3.2 | x2.8 | x2.4 |
| mandelbrot, 48 uneven rows (0.32s) | x2.4 | x4.5 | x5.3 | x6.9 |
| JSONL aggregation, 400k lines (0.25s) | x1.6 | x2.8 | x2.4 | x2.0 |

Notes:

* **Skewed load** is bounded by the heavy element itself (optimal makespan
  ~x2.8 here); demand-driven distribution saturates that bound from
  `lanes: 4` on. Blind round-robin caps at ~x2.4 because elements queued
  behind the heavy one cannot move to an idle worker.
* **JSONL parsing is allocation-bound**, and allocation throughput scales
  worse across Ractors than pure computation, so extra lanes stop helping
  early. Pre-aggregate in the workers and keep boundary objects small.
* The recurring dip at `lanes: 8` is a machine artifact of this hybrid
  P/E-core laptop under WSL2 (eight busy workers get placed badly), not a
  property of the lane count. Expect ±20% run-to-run variance from clock
  scaling in every cell.

**Message granularity** — trivial filter over 100k short lines:

| serial | 1 line = 1 message | `batch: 1000` |
|---|---|---|
| 0.011s | 1.56s | 0.021s |

Per-element messages cost ~1µs each (sync + copy + envelope); batching
amortizes that ~75x. Rule of thumb: aim for >= 1ms of work per message,
via `batch:` or by chunking the source.

**Source throttling** — infinite source, `pipe(lanes: 2){ it }.first(3)`:
the source's `each` is invoked tens to a few hundred times (it tracks
what the workers actually consume before the cancellation lands). A
push-fed pipeline reads hundreds of thousands of elements in the same
window.

## vs the parallel gem

The [parallel](https://github.com/grosser/parallel) gem is a data-parallel
`map` over a ready-made collection using forked processes (or threads);
Ractor::Pipeline is an in-process streaming topology. On the overlapping
map-shaped workloads (`examples/vs_parallel.rb`, same machine/Ruby as
above, parallel 2.1.0):

| workload | Ractor::Pipeline | Parallel processes | Parallel threads |
|---|---|---|---|
| 32 x fib(28), 16-way | **x6.6** | x4.6 | x1.0 (GVL) |
| skewed load, 4-way | x3.3 | x3.4 | — |
| JSONL aggregation, 8-way | **x2.5** | x1.7 | x0.8 |
| 10k trivial jobs, 4-way | 0.002s | 0.510s | — |

* Threads cannot help CPU-bound work under the GVL; processes and Ractors
  both can.
* Both distribute work by demand, so skewed load balances equally well.
* Ractor messages are in-process copies, cheaper than fork + Marshal over
  pipes: the gap widens when jobs carry data (JSONL: x2.5 vs x1.7) and
  becomes decisive for small jobs (250x on trivial jobs, thanks to
  `batch:` and reusable workers).
* What Parallel cannot express at all: multi-stage pipelines (stages
  running concurrently), infinite/throttled sources, and early
  cancellation of a running stream. What Parallel gives you instead:
  full process isolation and compatibility with any Ruby object without
  shareability rules.

## License

MIT. See [LICENSE.txt](LICENSE.txt).
