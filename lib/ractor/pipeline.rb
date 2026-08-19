# frozen_string_literal: true

require_relative "pipeline/version"

# Ractor::Pipeline: a DSL to build stream processing pipelines with Ractors.
#
#   include Ractor::Pipeline
#
#   stream(File.foreach(name)).
#     filter_pipe(lanes: 4){ it.include?("foo") }.
#     reduce([0, 0, 0]) do |(lines, words, bytes), line|
#       [lines + 1, words + line.scan(/\S+/).size, bytes + line.bytesize]
#     end
#
# Execution model:
#   * stream(source, batch: k) feeds the source into the pipeline from a
#     Thread in the caller Ractor, k elements per message (default 1).
#     Batching is transparent: stage blocks always see single elements.
#   * pipe/filter_pipe/flat_pipe are persistent Ractor stages; each stage
#     spawns `lanes:` (default 1) worker Ractors at terminal-operation
#     time. Workers process many elements; no Ractor is created per
#     element.
#   * Boundaries into a multi-lane stage (lanes > 1) are demand-driven
#     (pull): a lane worker sends a ready token upstream when it can take
#     more work, and producers send a batch only to a worker they hold a
#     token for (CREDIT tokens per producer/consumer pair). A busy worker
#     stops sending tokens, so work never piles up behind it. Boundaries
#     with a single consumer (lanes: 1 stages, and the terminal) are plain
#     push.
#   * When the head stage is multi-lane, the feeder reads the source only
#     while it holds tokens, so a fast or infinite source cannot flood the
#     pipeline.
#   * tee broadcasts each element to every branch; branch outputs are
#     merged (unordered) into one downstream stream.
#   * reduce/each/to_a/count/first/join are terminal operations executed
#     in the caller Ractor (join runs the pipeline purely for its side
#     effects, discarding output).
#
# Semantics notes:
#   * Ordering: a chain of lanes: 1 stages preserves input order (also
#     with batch:). Multi-lane stages are unordered.
#   * End of stream is an in-band EOS message: each worker counts EOS from
#     its upstream producers, drains its buffered output, and then
#     broadcasts EOS downstream, so fan-in and fan-out shut down cleanly.
#   * Cancellation (first, exceptions): the terminal closes its out_port
#     and broadcasts a cancel message to every worker. Workers exit
#     immediately, dropping their backlog; sends to closed ports raise
#     Ractor::ClosedError and cascade the closure upstream (SIGPIPE-style).
#   * Non-shareable objects are deep-copied at each Ractor boundary
#     (Ractor::Port#send default). tee copies an element once per branch.
#   * Stage blocks are isolated with Ractor.shareable_proc at DSL
#     construction time: they may capture shareable outer values (they are
#     snapshotted), and capturing a non-shareable value raises
#     Ractor::IsolationError at pipe/filter_pipe/flat_pipe call time.
#   * An exception raised in a stage block cancels the pipeline and is
#     re-raised by the terminal operation in the caller Ractor.
#   * Push links are unbounded (no backpressure); pull links are bounded
#     by CREDIT batches per producer/consumer pair.
#
# Message protocol (one input Port per worker; messages are tagged):
#   [:init, groups, ups, n_producers, batch_size]
#            groups: [[ports, pull?], ...] one per fan-out destination
#   [:data, batch, from]   # from: replenish target on pull links, else nil
#   [:ready, port]         # demand token (pull links only)
#   [:eos]                 # counted per upstream producer
#   [:failure, exception]  # forwarded to the terminal, which raises it
#   [:cancel]
class Ractor
  module Pipeline
    class Error < StandardError; end

    # Raised in a stage block to drop the current element:
    #
    #   pipe{ Integer(it) rescue raise(SKIP) }
    #   pipe{ raise SKIP if broken?(it); fix(it) }
    #
    # SKIP is for exceptional cases (systematic thinning is what
    # filter_pipe is for), so it is priced accordingly: the happy path
    # costs nothing and each skip costs one exception (~0.5us). It
    # inherits Exception, not StandardError, so a bare `rescue => e` in
    # the block cannot swallow it by accident.
    class SKIP < Exception; end

    CREDIT = 2 # ready tokens per (producer, consumer) pair on pull links

    STOP = Object.new # throw tag for early termination (caller Ractor only)
    private_constant :STOP

    Stage = Struct.new(:kind, :lanes, :job, :branches)

    # Common stage-building vocabulary for Source and Branch.
    module Stages
      def stages = @stages ||= []

      # Apply the block to each element and send the return value downstream.
      def pipe(lanes: 1, &block)
        add_stage(:pipe, lanes, block)
      end

      # Send the element itself downstream iff the block returns truthy.
      def filter_pipe(lanes: 1, &block)
        add_stage(:filter_pipe, lanes, block)
      end

      # The block returns an each-able object; each of its elements is sent
      # downstream (1 input -> N outputs).
      def flat_pipe(lanes: 1, &block)
        add_stage(:flat_pipe, lanes, block)
      end

      # Broadcast each element to every branch. Branch outputs are merged
      # (unordered) into one downstream stream.
      #
      #   stream(src).tee(
      #     pipe{ A(it) },
      #     pipe{ B(it) },
      #   ).to_a
      def tee(*branches)
        branches.each do |br|
          raise ArgumentError, "tee branch must be a stage list (use pipe{}/filter_pipe{})" unless Branch === br
          raise ArgumentError, "tee branch must have at least one stage" if br.stages.empty?
        end
        stages << Stage.new(:tee, nil, nil, branches.map(&:stages))
        self
      end

      private def add_stage(kind, lanes, block)
        raise ArgumentError, "no block given" unless block
        raise ArgumentError, "lanes: must be an Integer >= 1" unless Integer === lanes && lanes >= 1

        # Isolate the block here so that capturing outer variables fails
        # early, at DSL construction time.
        job = Ractor.shareable_proc(&block)
        stages << Stage.new(kind, lanes, job)
        self
      end
    end

    # A source-less pipeline fragment, used as a tee branch.
    class Branch
      include Stages
    end

    # A pipeline with a source. Terminal operations are defined here.
    class Source
      include Stages

      def initialize(source, batch: 1)
        raise ArgumentError, "source must respond to #each" unless source.respond_to?(:each)
        raise ArgumentError, "batch: must be an Integer >= 1" unless Integer === batch && batch >= 1
        @source = source
        @batch = batch
      end

      # -- terminal operations (executed in the caller Ractor) -----------

      def each(&block)
        raise ArgumentError, "no block given" unless block
        run { |msg| block.call(msg) }
        self
      end

      def reduce(initial, &block)
        raise ArgumentError, "no block given" unless block
        acc = initial
        run { |msg| acc = block.call(acc, msg) }
        acc
      end

      def to_a
        result = []
        run { |msg| result << msg }
        result
      end

      def count
        n = 0
        run { n += 1 }
        n
      end

      # Runs the pipeline for its side effects, discarding all output, and
      # returns self when the stream is fully processed -- like
      # Thread#join, but for a pipeline (conceptually `> /dev/null`).
      def join
        run { }
        self
      end

      # Terminates the pipeline as soon as enough elements are received.
      def first(n = nil)
        want = n || 1
        result = []
        if want > 0
          run do |msg|
            result << msg
            throw STOP, true if result.size >= want
          end
        end
        n ? result : result.first
      end

      private

      # A concrete stage placement in the wiring graph. downs holds the
      # fan-out destinations (Node or :out); producers the upstream Nodes
      # (empty = fed by the source feeder).
      Node = Struct.new(:kind, :lanes, :job, :downs, :producers, :ports)

      # Builds Nodes for a stage list, from downstream to upstream.
      # Returns the head destinations (what the previous producer feeds).
      def plan(stage_list, downs, nodes)
        stage_list.reverse_each.inject(downs) do |dwn, st|
          if st.kind == :tee
            st.branches.flat_map { |br_stages| plan(br_stages, dwn, nodes) }
          else
            node = Node.new(st.kind, st.lanes, st.job, dwn, [])
            nodes << node
            [node]
          end
        end
      end

      def run
        nodes = []
        heads = plan(stages, [:out], nodes)
        nodes.each do |node|
          node.downs.each { |dest| dest.producers << node if Node === dest }
        end
        n_out = nodes.sum { |node| node.downs.include?(:out) ? node.lanes : 0 }
        n_out = 1 if stages.empty? # fed directly by the feeder

        out_port = Ractor::Port.new
        feed_port = Ractor::Port.new
        ctrl = Ractor::Port.new

        # spawn workers (they wait for [:init]) and collect their ports
        nodes.each do |node|
          node.lanes.times do |i|
            Ractor.new(ctrl, node.job, node.kind, name: "pipeline/#{node.kind}[#{i}]") do |ctrl, job, kind|
              Ractor::Pipeline.worker_loop(ctrl, job, kind)
            end
          end
          node.ports = node.lanes.times.map { ctrl.receive }
        end
        all_ports = nodes.flat_map(&:ports)

        # wire up
        group_for = ->(dest) do
          dest == :out ? [[out_port], false] : [dest.ports, dest.lanes > 1]
        end
        nodes.each do |node|
          groups = node.downs.map(&group_for)
          ups = if node.lanes > 1
                  node.producers.empty? ? [feed_port] : node.producers.flat_map(&:ports)
                else
                  []
                end
          npro = node.producers.empty? ? 1 : node.producers.sum(&:lanes)
          node.ports.each { |port| port << [:init, groups, ups, npro, @batch] }
        end
        head_groups = heads.map(&group_for)

        feeder = Thread.new { feed(feed_port, head_groups) }

        eos = 0
        early = catch(STOP) do
          loop do
            msg = out_port.receive
            case msg[0]
            when :data
              msg[1].each { |obj| yield obj }
            when :eos
              eos += 1
              break if eos == n_out
            when :failure
              shutdown(feeder, out_port, all_ports)
              raise msg[1]
            end
          end
          false
        end

        if early
          shutdown(feeder, out_port, all_ports)
        else
          feeder.join
          out_port.close
        end
        nil
      end

      # Feed the source, batching elements. Pull destinations receive a
      # batch only when they granted a token, so the feeder never runs far
      # ahead of a multi-lane head stage; push destinations are direct.
      def feed(feed_port, head_groups)
        tokens = Pipeline.token_pools(head_groups)
        port_group = {}
        head_groups.each_with_index do |(ports, pull), gi|
          ports.each { |port| port_group[port] = gi } if pull
        end

        fill = lambda do |gi|
          until (port = tokens[gi].take)
            msg = feed_port.receive
            case msg[0]
            when :ready then tokens[port_group[msg[1]]].add(msg[1])
            when :cancel then throw :cancelled
            end
          end
          port
        end
        emit = lambda do |buf|
          head_groups.each_with_index do |(ports, pull), gi|
            if pull
              fill.call(gi) << [:data, buf, feed_port]
            else
              ports.first << [:data, buf, nil]
            end
          end
        end

        catch(:cancelled) do
          begin
            buf = []
            @source.each do |obj|
              buf << obj
              if buf.size >= @batch
                emit.call(buf)
                buf = []
              end
            end
            emit.call(buf) unless buf.empty?
          rescue Ractor::ClosedError
            next # cancelled
          rescue Exception => e
            head_groups.first.first.first << [:failure, e] rescue nil
          end
          head_groups.each { |ports, _| ports.each { |port| port << [:eos] rescue nil } }
        end
      end

      # Cancel: wake every worker (they may be idle or waiting for demand)
      # and close out_port so in-flight sends fail and cascade upstream.
      def shutdown(feeder, out_port, all_ports)
        feeder.kill
        feeder.join
        all_ports.each { |port| port << [:cancel] rescue nil }
        out_port.close
      end
    end

    # Demand tokens of one pull group. Tokens are granted round-robin over
    # the consumers (not in token arrival order): each consumer sends its
    # CREDIT initial tokens in a burst, and handing them out in arrival
    # order would cluster consecutive batches on the same worker.
    class TokenPool
      def initialize
        @counts = Hash.new(0)
        @order = []
      end

      def add(port)
        @order << port unless @counts.key?(port)
        @counts[port] += 1
      end

      def take
        @order.size.times do
          port = @order.shift
          @order.push(port)
          return port if @counts[port] > 0 && (@counts[port] -= 1 || true)
        end
        nil
      end
    end

    class << self
      def token_pools(groups)
        groups.map { TokenPool.new }
      end

      # The main loop of a stage worker. Stateless between streams except
      # for the demand bookkeeping of its (per-run) wiring.
      def worker_loop(ctrl, job, kind)
        in_port = Ractor::Port.new
        ctrl << in_port

        # wait for [:init]; queue anything that arrives earlier
        early = []
        msg = in_port.receive
        until msg[0] == :init
          early << msg
          msg = in_port.receive
        end
        _, groups, ups, n_producers, batch_size = msg

        tokens = Pipeline.token_pools(groups) # demand tokens, per pull group
        pending = groups.map { [] }           # batches waiting for a token
        port_group = {}
        groups.each_with_index do |(ports, pull), gi|
          ports.each { |port| port_group[port] = gi } if pull
        end
        eos = 0

        dispatch = lambda do |batch|
          groups.each_with_index do |(ports, pull), gi|
            if !pull
              ports.first << [:data, batch, nil]
            elsif (port = tokens[gi].take)
              port << [:data, batch, in_port]
            else
              pending[gi] << batch
            end
          end
        end

        handle = lambda do |m|
          case m[0]
          when :data
            begin
              case kind
              when :pipe
                out = []
                m[1].each do |obj|
                  out << job.call(obj)
                rescue SKIP
                end
                dispatch.call(out) unless out.empty?
              when :filter_pipe
                out = m[1].select do |obj|
                  begin
                    job.call(obj)
                  rescue SKIP
                    false
                  end
                end
                dispatch.call(out) unless out.empty?
              when :flat_pipe
                out = []
                m[1].each do |obj|
                  job.call(obj).each { |o| out << o }
                rescue SKIP
                end
                out.each_slice(batch_size) { |slice| dispatch.call(slice) }
              end
            rescue Ractor::ClosedError
              raise
            rescue Exception => e
              groups.first.first.first << [:failure, e] rescue nil
            end
            (m[2] << [:ready, in_port] rescue nil) if m[2] # replenish credit
          when :ready
            gi = port_group[m[1]]
            if (batch = pending[gi].shift)
              m[1] << [:data, batch, in_port]
            else
              tokens[gi].add(m[1])
            end
          when :eos
            eos += 1
          when :failure
            groups.first.first.first << m rescue nil # pass through
          when :cancel
            throw :cancelled
          end
        end

        catch(:cancelled) do
          ups.each { |up| CREDIT.times { up << [:ready, in_port] rescue nil } }
          early.each { |m| handle.call(m) }
          handle.call(in_port.receive) until eos == n_producers
          # all producers finished: drain buffered batches, then EOS
          handle.call(in_port.receive) until pending.all?(&:empty?)
          groups.each { |ports, _| ports.each { |port| port << [:eos] rescue nil } }
        end
      rescue Ractor::ClosedError
        # a consumer is gone: the stream was cancelled (SIGPIPE-style)
      end
    end

    # -- DSL entry points ------------------------------------------------

    def stream(source, batch: 1) = Source.new(source, batch:)

    # A stream of exactly one element: stream1(obj) == stream([obj]).
    # Use it to flow an object (even an each-able one) as a single element.
    def stream1(obj) = Source.new([obj])

    # Source-less fragments for tee branches.
    def pipe(lanes: 1, &block) = Branch.new.pipe(lanes:, &block)
    def filter_pipe(lanes: 1, &block) = Branch.new.filter_pipe(lanes:, &block)
    def flat_pipe(lanes: 1, &block) = Branch.new.flat_pipe(lanes:, &block)

    module_function :stream, :stream1, :pipe, :filter_pipe, :flat_pipe
  end
end
