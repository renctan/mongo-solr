#! /usr/local/bin/ruby

# A simple script that tries to measure the time it takes for to update large documents to
# Solr.
#
# Experimental Setup:
# The test generates a document with 3 fields, each around 1MB in size (by default) and
# inserts it to MongoDB. Every document insert is unique, by simply modifying certain
# characters among one of the fields chosen randomly. The document size being inserted
# is fixed on the entire test.
#
# The test forks a child process for the document insertion and also measures the time
# it takes to perform the inserts while the parent process tries to perform a sync. The
# insert are performed with the safe option on to make a fair comparison since the
# synchronization process needs to wait for the oplog entry to appear before it can perform
# the updates.

require_relative "../proj"

require "stringio"
require "rsolr"
require "#{PROJ_SRC_PATH}/solr_synchronizer"
require "#{PROJ_SRC_PATH}/argument_parser"
require "#{PROJ_SRC_PATH}/config_writer"
require "#{PROJ_SRC_PATH}/solr_config_const"
require "#{PROJ_SRC_PATH}/util"

TEST_DB = "MongoSolrUpdateTestBenchmark"
TEST_COLLECTION = "sink"
TEST_NS = "#{TEST_DB}.#{TEST_COLLECTION}"
DEFAULT_DOC_COUNT = 5

# Extracts the command line arguments.
#
# @param args [Array] the ARGV array
#
# @return [OpenStruct] a structure that contains the options.
#
# @see MongoSolr::ArgumentParser#parse_options
def parse_options(args)
  MongoSolr::ArgumentParser.parse_options(args) do |opts, options|
    options.docs = DEFAULT_DOC_COUNT
    options.solr_server = "http://localhost:8983/solr"
    options.field_size = 20

    opts.on("-s", "--solr SERVER", "The location of the Solr server.",
            "Defaults to #{options.solr_server}") do |server|
      options.solr_server = server
    end

    opts.separator ""
    opts.on("--doc_count COUNT", Integer,
            "The number of documents to insert for",
            "the test. Defaults to #{options.docs}.") do |num|
      options.docs = num
    end

    opts.separator ""
    opts.on("--field_size COUNT", Integer,
            "The size of the strings to insert per",
            "field in number of bits. Defaults to #{options.field_size}.") do |num|
      options.field_size = num
    end
  end
end

# A simple class that creates a document with large randomly generated strings.
class RandomDocGen
  def initialize(field_size_bit_count)
    @field_size = 2**field_size_bit_count

    @doc = {
      DELETE_THIS_KEY => DELETE_THIS_VALUE,
      "trash" => {
        "random gibberish" => rand_string(@field_size),
        "misc garbage" => rand_string(@field_size)
      },
      "other" => rand_string(@field_size)
    }
  end

  # @return [String] the Solr query string for documents generated by this class
  def self.solr_query
    "#{DELETE_THIS_KEY}:#{DELETE_THIS_VALUE}"
  end

  # @return [Hash] a randomly generated doc
  def generate_doc
    # Make sure that there is no _id key, otherwise, the insert to db will fail since
    # it won't accept new entries that the same _id as the exisiting ones.
    @doc.delete(:_id)

    str = case rand(3)
          when 0 then @doc["trash"]["random gibberish"]
          when 1 then @doc["trash"]["misc garbage"]
          else @doc["other"]
          end

    mutate_string(str, 10, @field_size)
    return @doc
  end

  ######################################################
  private
  # Subset of readable ASCII characters with some amount of space scattered around to
  # increase chances of having spaces
  READABLE_CHARS = "1234567890 abcdefghijklm nopqrstuvwxyz ABCDEFGHIJKLM NOPQRSTUVWXYZ " +
    "!\"#$\%&' ()~)=~|-^\\@ [;:],./_?> <}*+{`"
  READABLE_CHARS_SIZE = READABLE_CHARS.size
  DELETE_THIS_KEY = "delete_this"
  DELETE_THIS_VALUE = "#{TEST_DB}_delete_this"

  def rand_char
    READABLE_CHARS[rand(READABLE_CHARS_SIZE)]
  end

  # @param size [Integer] length of the string to generate.
  #
  # @return [String] a random generating string
  def rand_string(size)
    str = StringIO.new
    (size).times { str << rand_char }

    return str.string
  end

  # Randomly mutates a string.
  #
  # @param str [String] the string to mutate
  # @param distance [Integer] should be at least size/2.
  # @param size [Integer] size of the string
  def mutate_string(str, distance, size)
    pos = rand(size)
    tail_space = size - pos

    if tail_space < distance then
      pos = size - distance
    end

    str[pos, distance] = rand_string(distance)
  end
end

if $0 == __FILE__ then
  include MongoSolr::Util

  options = parse_options(ARGV)
  mongo = Mongo::Connection.new(options.mongo_loc, options.mongo_port)
  solr_loc = options.solr_server

  solr_client = RSolr.connect(:url => solr_loc)
  max_doc = options.docs
  doc_gen = RandomDocGen.new(options.field_size)

  config_coll = mongo[TEST_DB]["msolr"]
  config_coll.insert({ MongoSolr::SolrConfigConst::SOLR_URL_KEY => solr_loc,
                       MongoSolr::SolrConfigConst::NS_KEY => TEST_NS })
  config_writer = MongoSolr::ConfigWriter.new(config_coll, solr_loc)
  oplog_coll = get_oplog_collection(mongo, :auto)


  # Variables used inside sync block
  pid = nil
  start_time = nil

  opt = {
    :mode => options.mode,
    :err_retry_interval => options.err_interval,
    :auto_dump => options.auto_dump,
    :interval => options.interval,
    :logger => Logger.new("/dev/null"),
    :ns_set => { TEST_NS => {} }
  }

  docs_indexed = 0

  solr = MongoSolr::SolrSynchronizer.new(solr_client, mongo, oplog_coll, config_writer, opt)
  solr.sync do |mode, doc_count|
    if mode == :finished_dumping then
      start_time = Time.now

      pid = Process.fork do
        mongo2 = Mongo::Connection.new(options.mongo_loc, options.mongo_port)
        coll = mongo2.db(TEST_DB).collection(TEST_COLLECTION)
        max_doc.times { coll.insert(doc_gen.generate_doc(), :safe => true) }

        doc_insert_end_time = Time.now
        puts "It took child process #{doc_insert_end_time - start_time}" +
          " secs to finish inserting the docs."
      end
    elsif mode == :sync then
      # Note: doc_count is just the count of docs inserted for the batch
      docs_indexed += doc_count

      if !(docs_indexed < max_doc) then
        end_time = Time.now
        puts "It took #{end_time - start_time} secs to index #{docs_indexed} docs."
        break
      end
    end
  end

  Process.wait(pid)
  solr_client.delete_by_query(RandomDocGen.solr_query)
  solr_client.commit
  mongo.drop_database(TEST_DB)
end

