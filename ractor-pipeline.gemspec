# frozen_string_literal: true

require_relative "lib/ractor/pipeline/version"

Gem::Specification.new do |spec|
  spec.name = "ractor-pipeline"
  spec.version = Ractor::Pipeline::VERSION
  spec.authors = ["Koichi Sasada"]
  spec.email = ["ko1@atdot.net"]

  spec.summary = "A DSL to build stream processing pipelines with Ractors."
  spec.description = "Ractor::Pipeline provides a shell-pipeline-like DSL " \
                     "(stream/pipe/filter_pipe/flat_pipe/tee/lanes) where each " \
                     "stage is a persistent Ractor, making the execution topology " \
                     "visible in the code. Multi-lane stages are fed by demand " \
                     "(pull) and sources can be batched transparently."
  spec.homepage = "https://github.com/ko1/ractor-pipeline"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 4.0" # Ractor::Port, Ractor.shareable_proc

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ Gemfile .gitignore test/])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # Uncomment to register a new dependency of your gem
  # spec.add_dependency "example-gem", "~> 1.0"

  # For more information and examples about making a new gem, check out our
  # guide at: https://bundler.io/guides/creating_gem.html
end
