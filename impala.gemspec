# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'impala/version'

Gem::Specification.new do |gem|
  gem.name          = "impala"
  gem.version       = Impala::VERSION
  gem.authors       = ["Colin Marc"]
  gem.email         = ["colinmarc@gmail.com"]
  gem.description   = %q{A ruby client for Cloudera's Impala}
  gem.summary       = %q{A ruby client for Cloudera's Impala}
  gem.homepage      = "https://github.com/colinmarc/impala-ruby"

  gem.add_dependency('thrift', '~> 0.9')

  gem.add_development_dependency('rake')
  gem.add_development_dependency('eden')
  gem.add_development_dependency('pry')

  gem.add_development_dependency('minitest')
  gem.add_development_dependency('mocha')

  gem.add_development_dependency('yard')
  gem.add_development_dependency('redcarpet')
  gem.add_development_dependency('github-markup')

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
end
