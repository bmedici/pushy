# coding: utf-8
Gem::Specification.new do |spec|
  # Project version
  spec.version                      = "0.29.0"

  # Project description
  spec.name                         = "pushyd"
  spec.authors                      = ["Bruno MEDICI"]
  spec.email                        = "opensource@bmconseil.com"
  spec.description                  = "A nice proxy listenning to a RabbitMQ bus, repeating selected messages in POST requests when filters match routing patterns"
  spec.summary                      = spec.description
  spec.homepage                     = "http://github.com/bmedici/pushyd"
  spec.licenses                     = ["MIT"]
  spec.date                         = Time.now.strftime("%Y-%m-%d")

  # List files and executables
  spec.files                        = `git ls-files -z`.split("\x0")
  spec.executables                  = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths                = ["lib"]
  spec.required_ruby_version        = ">= 2.2"

  # Development dependencies
  spec.add_development_dependency   "bundler", "~> 1.6"
  spec.add_development_dependency   "rake"
  spec.add_development_dependency   "rspec"
  spec.add_development_dependency   "http", "~> 2.0"
  spec.add_development_dependency   "rubocop"
  # spec.add_development_dependency "pry"

  # Runtime dependencies
  # spec.add_runtime_dependency       "bmc-daemon-lib"
  spec.add_runtime_dependency       "bmc-daemon-lib", "~> 0.10"
  spec.add_runtime_dependency       "daemons"
  spec.add_runtime_dependency       "json"
  spec.add_runtime_dependency       "bunny", "~> 2.3"
  spec.add_runtime_dependency       "rest-client", "~> 1.8"
  spec.add_runtime_dependency       "api-auth"
  spec.add_runtime_dependency       "terminal-table"
  spec.add_runtime_dependency       "newrelic_rpm", "~> 4.0"
  spec.add_runtime_dependency       "rollbar"
end
