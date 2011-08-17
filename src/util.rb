require "open-uri"
require "bson"
require_relative "exception"

module MongoSolr
  MASTER_SLAVE_OPLOG_COLL_NAME = "oplog.$main"
  REPL_SET_OPLOG_COLL_NAME = "oplog.rs"

  module Util
    # Helper method for authenticating to a database.
    #
    # @param db_connection [Mongo::Connection] A connection to a MongoDB server
    # @param db_pass [Hash] ({}) The hash with database names as keys and a hash
    #   containing the authentication data as values with the format:
    #
    #   { :user => "foo", :pwd => "bar" }
    #
    #   Note that if the hash contains an account to the admin db, the other accounts
    #   will be ignored.
    def authenticate_to_db(db_connection, db_pass)
      # TODO: handle replica sets authentication!

      admin_auth = db_pass["admin"]

      unless admin_auth.nil? then
        db_connection.db("admin").authenticate(admin_auth[:user], admin_auth[:pwd], true)
      else
        db_pass.each do |db_name, auth|
          db_connection.db(db_name).authenticate(auth[:user], auth[:pwd], true)
        end
      end
    end

    # Checks if url can respond to a http/https request.
    #
    # @param url [String] The url to test
    # @param logger [Logger]
    #
    # @return [Boolean] true if url can be reached.
    def url_ok?(url, logger)
      ret = true

      begin
        response = open(url)
      rescue => e
        unless logger.nil? then
          logger.warn "Error encountered while trying to contact #{url}: #{e.message}"
        end

        ret = false
      end

      return ret
    end

    # Compare two timestamp object values
    #
    # @param ts1 [BSON::Timestamp]
    # @param ts2 [BSON::Timestamp]
    #
    # @return [Number] 0 if both are equal, -1 if ts1 < ts2 and 1 otherwise.
    def compare_bson_ts(ts1, ts2)
      if ts1.seconds < ts2.seconds then
        return -1
      elsif ts1.seconds > ts2.seconds then
        return 1
      elsif ts1.seconds == ts2.seconds then
        if ts1.increment < ts2.increment then
          return -1
        elsif ts1.increment > ts2.increment then
          return 1
        else
          return 0
        end
      end
    end

    # @param exception [Exception] the exception
    #
    # @return [String] the message which contains the description with the complete
    #   stack trace.
    def get_full_exception_msg(exception)
      return exception.message + exception.backtrace.join("\n")
    end

    # Gets the database name and collection name from a given namespace
    #
    # @param namespace [String] The namespace.
    #
    # @return [String, String] the two strings with the first one as the database name
    #   and the second one as the collection name.
    def get_db_and_coll_from_ns(namespace)
      split = namespace.split(".")
      db_name = split.first

      split.delete_at 0
      collection_name = split.join(".")

      return db_name, collection_name
    end

    # Attempts to upgrade a normal connection to a replica set connection.
    #
    # @param mongo [Mongo::Connection] The MongoDB connection to upgrade.
    #
    # @return [Mongo::Connection, Mongo::ReplSetConnection] the a replica set connection
    #   if the given host is a replica set member or a normal connection otherwise.
    def upgrade_to_replset(mongo)
      begin
        stat = mongo["admin"].command({ :replSetGetStatus => 1 })
        member_list = stat["members"].map do |member|
          host, port = member["name"].split(":")
          [host, port.to_i]
        end

        args = member_list
        args << { :rs_name => stat["set"] }

        mongo = Mongo::ReplSetConnection.new(*args)
      rescue Mongo::OperationFailure => e
        raise unless e.message =~ /--replSet/i
      end

      return mongo
    end

    # Converts a BSON timestamp object into an integer. Conversion rule is based from
    # the specs (http://bsonspec.org/#/specification).
    #
    # @param [BSON::Timestamp] the timestamp object
    #
    # @return [Number] an integer value representation of the BSON timestamp object.
    def bsonts_to_long(ts)
      return ((ts.seconds << 32) + ts.increment)
    end

    # Converts an integer value to a BSON timestamp
    #
    # @param [Number] the integer value to convert
    #
    # @return [BSON::Timestamp] the timestamp representation of the object
    def long_to_bsonts(val)
      seconds = val >> 32
      increment = val & 0xffffffff

      return BSON::Timestamp.new(seconds, increment)
    end

    # Gets the oplog collection of a Mongo instance.
    #
    # @param mongo [Mongo::Connection] The connection to the Mongo instance.
    # @param mode [Symbol] The mode of which the database server is running on. Recognized
    #   symbols: :master_slave, :repl_set, :auto
    #
    # @return [Mongo::Collection] the oplog collection
    #
    # @raise [OplogException]
    def get_oplog_collection(mongo, mode)
      oplog_coll = nil
      oplog_collection_name = case mode
                              when :master_slave then MASTER_SLAVE_OPLOG_COLL_NAME
                              when :repl_set then REPL_SET_OPLOG_COLL_NAME
                              else ""
                              end

      oplog_db = mongo.db("local")

      if oplog_collection_name.empty? then
        # Try to figure out which collection exists in the database
        candidate_coll = []

        begin
          candidate_coll << get_oplog_collection(mongo, :master_slave)
        rescue OplogException
          # Do nothing
        end
 
        begin
          candidate_coll << get_oplog_collection(mongo, :repl_set)
        rescue OplogException
          # Do nothing
        end

        if candidate_coll.empty? then
          raise OplogException, OPLOG_NOT_FOUND_MSG
        elsif candidate_coll.size > 1 then
          raise OplogException, OPLOG_AMBIGUOUS_MSG
        else
          oplog_coll = candidate_coll.first
        end
      else
        reply = oplog_db.command({ :collStats => oplog_collection_name },
                                 { :check_response => false })

        raise OplogException, OPLOG_NOT_FOUND_MSG unless reply["errmsg"].nil?

        oplog_coll = oplog_db.collection(oplog_collection_name)
      end

      return oplog_coll
    end

    # Compare between two objects using the rules specified for BSON objects:
    # http://www.mongodb.org/display/DOCS/What+is+the+Compare+Order+for+BSON+Types
    #
    # @param lhs [Object]
    # @param rhs [Object]
    #
    # @return [Integer] 1 if rhs > lhs, 0 if lhs == rhs and -1 otherwise.
    def bson_comp(lhs, rhs)
      lhs_rank = bson_type_rank lhs
      rhs_rank = bson_type_rank rhs

      comp = lhs_rank <=> rhs_rank

      if comp.zero? then
        if lhs == rhs then
          comp = 0
        elsif lhs < rhs then
          comp = -1
        else
          comp = 1
        end
      end

      return comp
    end

    private
    OPLOG_NOT_FOUND_MSG = "Cannot find oplog collection. Make sure that " +
      "you are connected to a server running on master/slave or replica set configuration."
    OPLOG_AMBIGUOUS_MSG = "Cannot determine which oplog to use. Please specify " +
      "the appropriate mode."

    # Get the rank of an object type in reference to the BSON comparison order.
    #
    # @param obj [Object] The object to rank.
    #
    # @return [Integer] an equivalent integer value for the relative rank of the object type.
    def bson_type_rank(obj)
      case obj
      when BSON::MinKey then -1000
      when obj.nil? then 0
      when Numeric then 1
      when Symbol, String then 2
      when Array then 4
      when BSON::Binary then 5
      when BSON::ObjectId then 6
      when Boolean then 7
      when Time, BSON::Timestamp then 8
      when Regexp then 9
      when BSON::MaxKey then 1000
      else 3 # Object
      end
    end
  end
end

