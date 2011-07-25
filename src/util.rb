require "open-uri"

module MongoSolr
  class Util
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
    def self.authenticate_to_db(db_connection, db_pass)
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
    def self.url_ok?(url, logger)
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
    def self.compare_bson_ts(ts1, ts2)
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
  end
end

