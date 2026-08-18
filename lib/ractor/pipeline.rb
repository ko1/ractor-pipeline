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
#   * stream(source) feeds each element of the source into the pipeline
#     (from a Thread in the caller Ractor, concurrently with the terminal
#     operation).
#   * pipe/filter_pipe/flat_pipe are persistent Ractor stages; each stage
#     spawns `lanes:` (default 1) worker Ractors which repeatedly
#     receive from their input Port and send results downstream.
#   * tee broadcasts each element to every branch; branch outputs are
#     merged (unordered) into one downstream stream.
#   * reduce/each/to_a/count/first are terminal operations executed in the
#     caller Ractor; no Ractor is created for them.
#
# Semantics notes (initial implementation):
#   * A stage with lanes: 1 preserves input order. A stage with
#     lanes: n > 1 is unordered: elements are distributed to the lanes by
#     round-robin and forwarded in completion order.
#   * End of stream is propagated by an EOS sentinel. Each worker knows
#     the number of upstream producers and finishes after receiving that
#     many EOS, then broadcasts EOS to all downstream ports.
#   * Non-shareable objects are deep-copied at each Ractor boundary
#     (Ractor::Port#send default). In particular, tee copies an element
#     once per branch.
#   * Stage blocks are isolated with Ractor.shareable_proc at DSL
#     construction time: they may capture shareable outer values (they are
#     snapshotted), and capturing a non-shareable value raises
#     Ractor::IsolationError at pipe/filter_pipe/flat_pipe call time.
#   * An exception raised in a stage block is wrapped in Failure, flows
#     down the pipeline, and is re-raised by the terminal operation in the
#     caller Ractor. Remaining workers are shut down by EOS.
#   * Ports are unbounded, so there is no backpressure (and no deadlock).
class Ractor
  module Pipeline
    class Error < StandardError; end

    # End-of-stream sentinel. A class object is shareable, so its identity
    # is preserved across Ractor boundaries.
    class EOS; end

    # Carries an exception raised in a stage block to the terminal operation.
    class Failure
      attr_reader :exception

      def initialize(exception)
        @exception = exception
      end
    end

    Stage = Struct.new(:kind, :lanes, :job, :branches, :n_producers)

    STOP = Object.new # throw tag for early termination (used in the caller Ractor only)
    private_constant :STOP

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

      def initialize(source)
        raise ArgumentError, "source must respond to #each" unless source.respond_to?(:each)
        @source = source
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

      # Wire up the pipeline, feed the source, and yield each output
      # element to the given block.
      def run
        out_port = Ractor::Port.new
        ctrl = Ractor::Port.new
        n_out = Pipeline.annotate(stages, 1)
        head_groups = Pipeline.spawn_stages(stages, [[out_port]], ctrl)

        # Feed the source concurrently so that terminal consumption
        # overlaps with production.
        feeder = Thread.new do
          rr = 0
          begin
            @source.each do |obj|
              head_groups.each { |group| group[rr % group.size] << obj }
              rr += 1
            end
          rescue Exception => e
            head_groups.first.first << Failure.new(e)
          ensure
            head_groups.each { |group| group.each { |port| port << EOS rescue nil } }
          end
        end

        eos = 0
        early = catch(STOP) do
          loop do
            msg = out_port.receive
            if EOS.equal?(msg)
              eos += 1
              break if eos == n_out
            elsif Failure === msg
              shutdown(feeder, out_port, head_groups)
              raise msg.exception
            else
              yield msg
            end
          end
          false
        end

        if early
          shutdown(feeder, out_port, head_groups)
        else
          feeder.join
          out_port.close
        end
        nil
      end

      # Cancel the stream: closing out_port makes the tail workers' sends
      # raise Ractor::ClosedError and the closure cascades upstream, so
      # busy workers stop without draining their backlog. Idle workers
      # (blocked on an empty queue, nothing to send) cannot see the
      # cascade, so EOS is still sent to wake and drain them.
      def shutdown(feeder, out_port, head_groups)
        feeder.kill
        feeder.join
        out_port.close
        head_groups.each { |group| group.each { |port| port << EOS rescue nil } }
      end
    end

    class << self
      # Records how many upstream producers feed each stage (the number of
      # EOS messages each worker must observe before finishing) and
      # returns the number of producers feeding whatever follows `stages`.
      def annotate(stages, n_in)
        stages.each do |st|
          st.n_producers = n_in
          if st.kind == :tee
            n_in = st.branches.sum { |br_stages| annotate(br_stages, n_in) }
          else
            n_in = st.lanes
          end
        end
        n_in
      end

      # Spawns worker Ractors from downstream to upstream.
      #
      # down_groups is an Array of port groups the current tail stage must
      # send to: one element per fan-out destination, each element being
      # the input ports of that destination's workers. An element is sent
      # to one (round-robin) port of every group; EOS is broadcast to all
      # ports of all groups.
      #
      # Returns the port groups for the pipeline head (what the source
      # feeder must send to).
      def spawn_stages(stages, down_groups, ctrl)
        stages.reverse_each do |st|
          if st.kind == :tee
            down_groups = st.branches.flat_map { |br_stages| spawn_stages(br_stages, down_groups, ctrl) }
          else
            st.lanes.times do |i|
              Ractor.new(ctrl, down_groups, st.n_producers, st.job, st.kind, i,
                         name: "pipeline/#{st.kind}[#{i}]") do |*args|
                Ractor::Pipeline.worker_loop(*args)
              end
            end
            # Workers create their own input Port (Port#receive is
            # restricted to the creating Ractor) and report it via ctrl.
            down_groups = [st.lanes.times.map { ctrl.receive }]
          end
        end
        down_groups
      end

      # The main loop of a persistent stage worker.
      def worker_loop(ctrl, down_groups, n_producers, job, kind, index)
        in_port = Ractor::Port.new
        ctrl << in_port

        rr = index # start position varies per worker to spread the load
        eos = 0
        emit = ->(obj) do
          down_groups.each { |group| group[rr % group.size] << obj }
          rr += 1
        end

        loop do
          msg = in_port.receive
          if EOS.equal?(msg)
            eos += 1
            break if eos == n_producers
          elsif Failure === msg
            emit.call(msg) # pass failures through to the terminal operation
          else
            begin
              case kind
              when :pipe
                emit.call(job.call(msg))
              when :filter_pipe
                emit.call(msg) if job.call(msg)
              when :flat_pipe
                job.call(msg).each { |obj| emit.call(obj) }
              end
            rescue Ractor::ClosedError
              raise
            rescue Exception => e
              emit.call(Failure.new(e))
            end
          end
        end

        # All upstream producers finished; propagate EOS downstream.
        down_groups.each { |group| group.each { |port| port << EOS rescue nil } }
      rescue Ractor::ClosedError
        # A downstream port is closed: the stream was cancelled (the
        # terminal operation closed its out_port, like SIGPIPE in a shell
        # pipeline). Exit without draining the backlog; our own ports
        # close with this Ractor, so the closure cascades upstream.
      end
    end

    # -- DSL entry points ------------------------------------------------

    def stream(source) = Source.new(source)

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
