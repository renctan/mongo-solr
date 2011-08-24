require File.expand_path("../lib/version", __FILE__)

Gem::Specification.new do |s|
  s.name = "msolr"

  s.version = MongoSolr::VERSION

  s.platform = Gem::Platform::RUBY
  s.summary = "MongoDB integration for Apache Solr."
  s.description = "A daemon that"

  s.require_paths = ["lib"]

  s.files = ["README.md", "msolr.gemspec", "solr-plugin.js"]
  s.files += Dir["lib/*.rb"]
  s.files += Dir["lib/msolr/*.rb"]
  s.files += Dir["lib/js/*.js"]
  s.files += ["bin/msolrd", "bin/msolrc"]

  s.executables = %w[msolrd msolrc]

  s.has_rdoc = false

  s.authors = ["Randolph Tan"]

  s.add_dependency("rsolr")
  s.add_dependency("mongo", [">= 1.3.1"])
  s.add_dependency("hamster")
  s.add_dependency("highline")
end

