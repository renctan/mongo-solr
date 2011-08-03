require "rubygems"
require "rake/testtask"

task :test do
  Rake::Task["test:all"].invoke
end

namespace "test" do
  desc "Runs all tests"
  task :all => [:js, :unit, :integration, :slow]

  desc "Runs the tests for the js Mongo Shell plugin"
  task :js do
    orig_dir = Dir.pwd
    Dir.chdir("test/js")
    output = system("ruby js_test.rb")
    puts output
    Dir.chdir orig_dir
  end

  desc "Runs the unit tests"
  Rake::TestTask.new(:unit) do |t|
    t.test_files = FileList["test/unit/*_test.rb"]
    t.verbose = true
  end

  desc "Runs the integration tests"
  Rake::TestTask.new(:integration) do |t|
    t.test_files = FileList["test/integration/*_test.rb"]
    t.verbose = true
  end

  desc "Runs the very slow tests"
  Rake::TestTask.new(:slow) do |t|
    t.test_files = FileList["test/slow_tests/*_test.rb"]
    t.verbose = true
  end
end

task :default do
  system("rake -T")
end

file "solr.js" do
  # Note: sequence is relevant. Dependent files should be placed before files it depend.
  js_files = %w[msolr_const util msolr_server msolr]
  js_opt_arr = js_files.map { |file| "--js=src/js/#{file}.js" }
  js_opt_str = js_opt_arr.join(" ")

  system("java -jar compiler.jar #{js_opt_str} --js_output_file=solr.js")
end

file "solr-plugin.js" => "solr.js" do |task|
  # Note: sequence is relevant. Dependent files should be placed before files it depend.
  js_files = task.prerequisites
  js_files << "src/js/msolr_plugin.js"
  js_opt_arr = js_files.map { |file| "--js=#{file}" }
  js_opt_str = js_opt_arr.join(" ")

  system("java -jar compiler.jar #{js_opt_str} --js_output_file=solr-plugin.js")
end

desc "Build the Mongo Shell plugin js file. Needs the Google Closure compiler jar file to be at the root directory."
task :build => "solr-plugin.js"

desc "Build the Mongo Shell plugin js file from scratch."
task :rebuild do
  rm FileList["*.js"]
  Rake::Task[:build].invoke
end

