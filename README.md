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
stream(File.foreach(name))
  .filter_pipe(lanes: 4){ it.include?("foo") }
  .reduce([0, 0, 0]) do |(lines, words, bytes), line|
    [lines + 1, words + line.scan(/\S+/).size, bytes + line.bytesize]
  end
```

```text
File.foreach ──> filter Ractor x4 ──> Port ──> reduce in caller
```

Requires Ruby 4.0+ (`Ractor::Port`, `Ractor.shareable_proc`). The Ractor API
is experimental, and so is this library.

## Overview

| DSL | Enumerable analogue | topology |
|---|---|---|
| `stream(src)` | `src.each` | source (fed from the caller) |
| `stream1(obj)` | `[obj].each` | single-element source |
| `.pipe{ f(it) }` | `map` | 1 persistent Ractor |
| `.pipe(lanes: n){}` | `map` | n Ractors, round-robin, unordered |
| `.filter_pipe{ pred(it) }` | `filter` | 1 Ractor (sends the original element) |
| `.flat_pipe{ enum }` | `flat_map` | 1 input -> N outputs |
| `.tee(pipe{}, pipe{})` | — | broadcast to branches, merged output |
| `.reduce(init){}` `.each{}` `.to_a` `.count` `.first(n)` | same | terminal, runs in the caller, no Ractor |

## API guide

`include Ractor::Pipeline` makes the vocabulary available
(`Ractor::Pipeline.stream(...)` also works). Stage-building methods return
the pipeline object, so they chain; nothing runs until a terminal operation
is called.

### `stream(source)`

Creates a pipeline whose input is each element of `source` (anything that
responds to `each`). Elements are fed from a background Thread in the
caller Ractor, concurrently with the terminal operation, so producing and
consuming overlap.

```ruby
stream([1, 2, 3])           # three elements
stream(1..)                 # infinite stream (terminate with .first etc.)
stream(File.foreach(name))  # one element per line
```

`stream1(obj)` is a shorthand for `stream([obj])`: it flows `obj` as a
single element, even when `obj` itself is each-able.

```ruby
stream1(config).pipe{ build(it) }.first
```

### `pipe(lanes: 1){ block }`

A processing stage: applies the block to each element and sends the return
value downstream. The current element is `it`. One stage = one persistent
Ractor (per lane) that processes many elements; Ractors are *not* created
per element.

```ruby
stream([1, 2, 3]).pipe{ it * 2 }.to_a  #=> [2, 4, 6]
```

With `lanes: n`, the stage becomes n worker Ractors. Elements are
distributed round-robin and forwarded in completion order (unordered; see
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
stream(file_names)
  .flat_pipe{ File.foreach(it) }            # 1 file -> N lines
  .filter_pipe(lanes: 4){ it.include?("Ractor") }
  .count
```

### `tee(branch, branch, ...)`

Broadcasts each element to *every* branch (not load balancing). Branches
are receiver-less fragments; their outputs are merged, unordered, into one
downstream stream. Non-shareable elements are copied once per branch.

```ruby
evens, odds = stream(1..10)
                .tee(
                  filter_pipe{ it.even? }.pipe{ [:even, it] },
                  filter_pipe{ it.odd?  }.pipe{ [:odd,  it] },
                )
                .reduce([[], []]) do |(evens, odds), (tag, n)|
                  tag == :even ? [evens << n, odds] : [evens, odds << n]
                end
```

### Terminal operations

Terminal operations run in the caller Ractor (no Ractor is created for
them) and start the pipeline. Their blocks are ordinary blocks — no
isolation restrictions, so a mutable accumulator is fine.

```ruby
.reduce(initial){ |acc, elem| ... }  # returns the final accumulator
.each{ |elem| ... }                  # yields each element, returns self
.to_a                                # collects into an Array
.count                               # number of output elements
.first                               # first element (stops the pipeline)
.first(n)                            # first n elements as an Array
```

`first` terminates early: it stops feeding the source, sends EOS through
the graph, and returns — safe to use with an infinite `stream(1..)`.

### Errors

* An exception raised in a stage block is wrapped, flows down the
  pipeline, and is re-raised by the terminal operation in the caller:

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

## Samples

### wc: `cat README.md | grep Ruby | wc`

```ruby
lines, words, bytes =
  stream(File.foreach("README.md"))
    .filter_pipe(lanes: 4){ it.include?("Ruby") }
    .reduce([0, 0, 0]) do |(l, w, b), line|
      [l + 1, w + line.scan(/\S+/).size, b + line.bytesize]
    end
```

### map-reduce: word ranking over many files

One file = one message, so the boundary cost is amortized over
file-size-scale work:

```ruby
top5 = stream(Dir.glob("**/*.c"))
         .pipe(lanes: 8) do
           tally = Hash.new(0)
           File.read(it).scan(/\w+/){ |w| tally[w] += 1 }
           tally
         end
         .reduce(Hash.new(0)) do |acc, tally|
           tally.each{ |w, n| acc[w] += n }
           acc
         end
         .max_by(5){ |_, n| n }
```

### Unordered results: carry an index and reassemble

```ruby
picture = stream(0...height)
            .pipe(lanes: 8){ [it, render_row(it)] }
            .reduce(Array.new(height)){ |acc, (y, row)| acc[y] = row; acc }
```

### Early termination of an infinite stream

```ruby
stream(1..).pipe{ it * it }.first(5)  #=> [1, 4, 9, 16, 25]
```

### Chunking for fine-grained sources

Per-line messages are dominated by copy/communication cost; chunk them:

```ruby
stream(File.foreach(name).each_slice(1000))
  .pipe(lanes: 4){ it.count{ |line| line.include?("VALUE") } }
  .reduce(0){ |acc, n| acc + n }
```

Runnable versions of these (plus performance measurements) are in
[examples/demo.rb](examples/demo.rb) and [examples/perf.rb](examples/perf.rb).

## Semantics notes (initial implementation)

* <a name="ordering"></a>**Ordering**: `lanes: 1` chains preserve input
  order. Multi-lane stages are unordered (elements overtake each other
  across lanes); within one lane, order is preserved.
* **Boundaries copy**: non-shareable objects are deep-copied at each Ractor
  boundary (`Ractor::Port#send` default); shareable objects are passed by
  reference.
* **End of stream** propagates via a sentinel: each worker counts EOS from
  its upstream producers and then broadcasts EOS downstream, so fan-in and
  fan-out shut down cleanly.
* **No backpressure**: ports are unbounded queues (and therefore there is
  no deadlock, but also no flow control for a slow consumer).

## Performance notes

* Message granularity dominates: one message per *line* of a file can be
  ~100x slower than one message per file or per 1000-line chunk. Amortize
  the boundary cost over enough work per message.
* Keep stage blocks allocation-light; allocation throughput scales worse
  across Ractors than pure computation.
* DSL overhead vs hand-written persistent Ractors is negligible when a
  message carries >= ~1ms of work.

## License

MIT. See [LICENSE.txt](LICENSE.txt).
