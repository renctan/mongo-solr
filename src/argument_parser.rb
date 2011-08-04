require "optparse"
require "ostruct"

module MongoSolr
  # A simple helper class for parsing command line options.
  class ArgumentParser
    # Parses the command line options.
    #
    # @param args [Array] The ARGV array.
    # @param block [Proc(OptionParser, OpenStruct)] An optional block 
    #
    # @return [OpenStruct] the option structure that contains the options.
    def self.parse_options(args, &block)
      # Initialize all parameters here.
      options = OpenStruct.new
      options.mongo_loc = "localhost"
      options.mongo_port = 27017
      options.mode = :auto
      options.interval = 1
      options.config_interval = 1
      options.err_interval = 10
      options.auto_dump = false
      options.auth = {}

      opt_parser = OptionParser.new do |opts|
        opts.banner = "Usage: #{__FILE__} [options]"

        opts.separator ""
        opts.separator "Specific options:"

        opts.separator ""
        opts.on("-d", "--mloc SERVER", "The location of the MongoDB server.",
                "The mongo connection string can also be specified here.",
                "(http://www.mongodb.org/display/DOCS/Connections)",
                "Defaults to #{options.mongo_loc}.") do |server|
          options.mongo_loc = server
        end

        opts.separator ""
        opts.on("-p", "--mport PORT_NUMBER", Integer,
                "The port number of the MongoDB server.",
                "Defaults to #{options.mongo_port}.") do |port_num|
          options.mongo_port = port_num
        end

        opts.separator ""
        opts.on("-m", "--mode MODE", "ms for master/slave or rs for",
                "replica set. Tries to automatically",
                "detect which mode to use by default.") do |mode|
          options.mode = case mode
                         when "ms" then :master_slave
                         when "rs" then :repl_set
                         else :unknown
                         end
        end

        opts.separator ""
        opts.on("-i", "--interval SECONDS", Float,
                "The number of seconds to wait before",
                "polling the oplog for more updates.",
                "Does not need to be an integer,",
                "but must be >= 1. Defaults to #{options.interval}.") do |seconds|
          options.interval = seconds
        end

        opts.separator ""
        opts.on("-e", "--err_interval SECONDS", Float,
                "The number of seconds to wait before",
                "retrying again when an error occured.",
                "Does not need to be an integer,",
                "but must be >= 1. Defaults to #{options.err_interval}.") do |seconds|
          options.err_interval = seconds
        end

        opts.separator ""
        opts.on("-c", "--conf_refresh SECONDS", Float,
                "The number of seconds to wait before",
                "polling the config DB for more updates.",
                "Does not need to be an integer,",
                "but must be >= 1. Defaults to #{options.config_interval}.") do |seconds|
          options.config_interval = seconds
        end

        opts.separator ""
        opts.on("-a", "--auto_dump", "Automatically performs a full DB",
                "dump when the oplog cursor gets too stale.",
                "The default behavior without this option",
                "is to raise an exception and terminate.") do
          options.auto_dump = true
        end

#        opts.separator ""
#        opts.on("-a", "--auth FILE_PATH",
#                "The file that contains authentication",
#                "credentials to the databases. The file",
#                "should have separate entries for each",
#                "database in one line with database",
#                "name, user name and password separated",
#                "by a comma. Sample file contents:",
#                " ",
#                "admin,root_user,root_password",
#                "local_db,user,strong_password",
#                " ",
#                "Note that spaces are valid characters in",
#                "user name and passwords so space characters",
#                "will not be ignored in the file. There are",
#                "currently issues for username and password",
#                "usernames that contains the comma (,)",
#                "character.") do |path|
#          options.auth = load_auth_file(path)
#        end

        yield opts, options if block_given?

        opts.separator ""
        opts.on_tail("-h", "--help", "Show this message") do
          puts opts
          exit
        end
      end

      opt_parser.parse!(args)
      return options
    end

    ############################################################################
    private

    # Build a hash structure from the contents of an authentication file that conforms with
    # the formatting for the opt[:db_pass] parameter for SolrSynchronizer#sync.
    #
    # @param file_path [String] the path of the authentication file.
    #
    # @return [Hash] the Hash object.
    def self.load_auth_file(file_path)
      auth_data = {}

      open(file_path).each_line do |line|
        columns = line.split(",")
        db_name = columns[0].chomp

        unless db_name.empty? then
          auth_data[columns[0]] = { :user => columns[1], :pwd => columns[2].chomp }
        end
      end

      return auth_data
    end
  end
end

