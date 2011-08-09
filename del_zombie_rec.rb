#! /usr/local/bin/ruby

# A simple script for permanently deleting the documents marked by MongoSolr as deleted.

if __FILE__ == $0 then
  # Copy and pasted from:
  # http://stackoverflow.com/questions/4333286/ruby-require-vs-require-relative-best-practice-to-workaround-running-in-both-r/4718414#4718414
  unless Kernel.respond_to?(:require_relative)
    module Kernel
      def require_relative(path)
        require File.join(File.dirname(caller[0]), path.to_str)
      end
    end
  end
end

require "rsolr"
require_relative "src/solr_synchronizer"

if __FILE__ == $0 then
  if ARGV.length != 1 then
    puts "Usage: #{__FILE__} <solr server location>"
  else
    solr_loc = ARGV[0]

    solr = RSolr.connect(solr_loc)
    solr.delete_by_query("#{MongoSolr::SolrSynchronizer::SOLR_DELETED_FIELD}:true")
    solr.commit
  end
end

