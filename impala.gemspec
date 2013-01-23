# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'impala/version'

Gem::Specification.new do |gem|
  gem.name          = "impala"
  gem.version       = Impala::VERSION
  gem.authors       = ["Colin Marc"]
  gem.email         = ["colinmarc@gmail.com"]
  gem.description   = %q{TODO: Write a gem description}
  gem.summary       = %q{TODO: Write a gem summary}
  gem.homepage      = ""

  gem.add_dependency('thrift', '~> 0.9')

  gem.add_development_dependency('rake')
  gem.add_development_dependency('eden')
  gem.add_development_dependency('pry')

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
end
