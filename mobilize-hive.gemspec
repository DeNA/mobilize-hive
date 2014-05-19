# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mobilize-hive/version'

Gem::Specification.new do |gem|
  gem.name          = "mobilize-hive"
  gem.version       = Mobilize::Hive::VERSION
  gem.authors       = ["Cassio Paes-Leme", "Ryosuke IWANAGA"]
  gem.email         = ["cpaesleme@dena.com", "riywo.jp@gmail.com"]
  gem.license       = "Apache License, Version 2.0"
  gem.homepage      = "http://github.com/DeNA/mobilize-hive"
  gem.description   = %q{Adds hive read, write, and run support to mobilize-hdfs}
  gem.summary       = %q{Adds hive read, write, and run support to mobilize-hdfs}

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.add_runtime_dependency "mobilize-hdfs","1.390"
end
