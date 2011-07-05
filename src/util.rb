require "mongo"

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
  end
end
