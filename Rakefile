require "rubygems"
require "rake/testtask"

task :test do
  Rake::Task["test:all"].invoke
end

namespace "test" do
  desc "Run all tests"
  task :all => [:js, :unit, :integration]

  desc "Run tests for the js Mongo Shell plugin"
  task :js do
    orig_dir = Dir.pwd
    Dir.chdir("test/js")
    output = system("ruby js_test.rb")
    puts output
    Dir.chdir orig_dir
  end

  Rake::TestTask.new(:unit) do |t|
    t.test_files = FileList["test/unit/*_test.rb"]
    t.verbose = true
  end

  Rake::TestTask.new(:integration) do |t|
    t.test_files = FileList["test/integration/*_test.rb"]
    t.verbose = true
  end
end

task :default do
  system("rake -T")
end

file "solr.js" do
  # Note: sequence is relevant. Dependent files should be placed before files it depend.
  js_files = %w[msolr_const msolr_db msolr_server msolr]
  js_opt_arr = js_files.map { |file| "--js=src/js/#{file}.js" }
  js_opt_str = js_opt_arr.join(" ")

  system("java -jar compiler.jar #{js_opt_str} --js_output_file=solr.js")
end

desc "Build the Mongo Shell plugin js file. Needs the Google Closure compiler jar file to be at the root directory."
task :build => "solr.js"

