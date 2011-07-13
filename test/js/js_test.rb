#! /usr/local/bin/ruby

# A simple script for running all the js tests. This should be executed on the same
# directory where this script is located.

# Load the test script on the Mongo Shell environment and execute it.
#
# @param file [String] The file name of the test script.
def do_test(file)
  puts "Testing #{file} ============================="
  puts `mongo --eval \"load(\\\"#{file}\\\")\"`
  puts ""
end

if __FILE__ == $0 then
  if ARGV.size > 0 then
    ARGV.each do |test|
      do_test test
    end
  else
    Dir.glob("*_test.js").each do |test|
      do_test test
    end
  end
end

