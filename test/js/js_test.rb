#! /usr/local/bin/ruby

# A simple script for running all the js tests.

if __FILE__ == $0 then
  Dir.glob("*_test.js").each do |test|
    puts "Testing #{test} ============================="
    puts `mongo --eval \"load(\\\"#{test}\\\")\"`
    puts ""
  end
end

