require "open-uri"

module MongoSolr
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

    # Attempts to check whether the connection is to a replica set.
    #
    # @param hostname [String] The hostname of the mongod instance to connect to.
    # @param port [Number] The port number of the mongod instance to connect to.
    #
    # @return [Mongo::Connection, Mongo::ReplSetConnection] the a replica set connection
    #   if the given host is a replica set member or a normal connection otherwise.
    def auto_detect_replset(hostname, port)
      mongo = Mongo::Connection.new(hostname, port)

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
  end
end

