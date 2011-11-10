spec = Gem::Specification.new do |s| 
  s.name = "snail-map-reduce"
  s.version = "0.0.1"
  s.author = "Avishek Sen Gupta"
  s.email = "avishek.sen.gupta@gmail.com"
  s.homepage = "http://avishek.net/blog"
  s.platform = Gem::Platform::RUBY
  s.summary = "Some description"
  s.files = `git ls-files`.split("\n")
  s.summary = %q{Snail is a single-threaded, in-memory, barebones MapReduce framework written in Ruby to quickly prototype and test parallel algorithms.}
  s.description = %q{Snail is a single-threaded, in-memory, barebones MapReduce framework written in Ruby to quickly prototype and test parallel algorithms.}
end

