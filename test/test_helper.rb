# frozen_string_literal: true

Warning[:experimental] = false # suppress "Ractor API is experimental"

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "ractor/pipeline"

require "test-unit"
