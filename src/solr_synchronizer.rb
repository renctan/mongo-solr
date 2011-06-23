require "rubygems"
require "rsolr"
require "mongo"
require "set"
require "logger"

require_relative "document_transform"
require_relative "exception"

module MongoSolr
  # A simple class utility for indexing an entire MongoDB database contents (excluding
  # administrative collections) to Solr.
  class SolrSynchronizer
    # [Logger] Set the logger to be used to output. Defaults to STDOUT.
    attr_writer :logger

    # Create a synchronizer instance.
    #
    # @param solr [RSolr::Client] The client to the Solr server to populate database documents.
    # @param mongo_connection [Mongo::Connection] The connection to the database to synchronize.
    # @param mode [Symbol] Mode of the MongoDB server connected to. Accepted symbols - :repl_set,
    #   :master_slave
    #
    # Example:
    #  mongo = Mongo::Connection.new("localhost", 27017)
    #  solr_client = RSolr.connect(:url => "http://localhost:8983/solr")
    #  solr = MongoSolr::SolrSynchronizer.new(solr_client, mongo, :master_slave)
    def initialize(solr, mongo_connection, mode)
      @solr = solr
      @db_connection = mongo_connection
      @mode = mode
      @logger = Logger.new(STDOUT)
    end

    # Continuously synchronizes the database contents with Solr. Please note that this is a
    # blocking call that contains an infinite loop.
    #
    # @option opt [number] :interval (1) The interval in seconds to wait before checking for
    #    new updates in the database
    # @option opt [Hash<Hash>] :db_pass ({}) The hash with database names as keys and a hash
    #   containing the authentication data as values with the format:
    #
    #     { :user => "foo", :pwd => "bar" }
    #
    # @param block [Proc(mode, doc_count)] An optional  procedure that will be called during
    #   certain conditions:
    #
    #   1. block(:finished_dumping, 0) will be called after dumping the contents of the database.
    #   2. block(:sync, doc_count) will be called everytime Solr is updated from the oplog.
    #      doc_count contains the number of docs synched with Solr so far.
    #
    # @raise [OplogException]
    #
    # Example:
    # auth = { "users" => { :user => "root", :pwd => "root" },
    #          "admin" => { :user => "admin", :pwd => "" } }
    # solr.sync({ :db_pass => auth, :interval => 1 })
    def sync(opt = {}, &block)
      db_pass = opt[:db_pass] || {}

      if authenticate_to_db("admin", db_pass) then
        db_pass = {}
      end

      if authenticate_to_db("local", db_pass) then
        db_pass.delete("local")
      end

      cursor = Mongo::Cursor.new(get_oplog_collection,
                                 { :tailable => true, :selector => {"op" => {"$ne" => "n"} } })

      # Go to the tail of cursor. Must find a better way moving the cursor to the latest entry.
      while cursor.next_document do
        # Do nothing
      end

      if @admin_authenticated then
        db_pass = {}
      else
        db_pass = opt[:dbpass] || {}
      end

      dump_db_contents(db_pass)
      yield :finished_dumping, 0 if block_given?

      update_interval = opt[:interval] || 1
      doc_count = 0

      loop do
        doc_batch = []

        while doc = cursor.next_document do
          unless filter_entry?(doc["ns"]) then
            doc_batch << doc
            doc_count += 1
          else
            @logger.debug "skipped oplog: #{doc}"
          end
        end

        update_solr(doc_batch)
        yield :sync, doc_count if block_given?

        sleep update_interval
      end
    end

    ############################################################################
    private
    SPECIAL_PURPOSE_MONGO_DB_NAME_PATTERN = /^(local|admin|config)$/
    SPECIAL_COLLECTION_NAME_PATTERN = /^system\./
    MASTER_SLAVE_OPLOG_COLL_NAME = "oplog.$main"
    REPL_SET_OPLOG_COLL_NAME = "oplog.rs"
    OPLOG_NOT_FOUND_MSG = "Cannot find oplog collection. Make sure that " +
      "you are connected to a server running on master/slave or replica set configuration."
    OPLOG_AMBIGUOUS_MSG = "Cannot determine which oplog to use. Please specify " +
      "the appropriate mode."

    # Dump the all contents of the MongoDB server (with the exception of special purpose
    # databases like admin and config) to be indexed to Solr. If db_pass is not empty, only
    # the databases with authentication data will be dumped.
    #
    # @param db_pass [Hash] ({}) The hash with database names as keys and a hash
    #   containing the authentication data as values with the format:
    #
    #     { :user => "foo", :pwd => "bar" }
    def dump_db_contents(db_pass = {})
      if db_pass.empty? then
        @db_connection.database_names.each do |db_name|
          unless db_name =~ SPECIAL_PURPOSE_MONGO_DB_NAME_PATTERN then
            dump_collections(@db_connection.db(db_name))
          end
        end
      else
        db_pass.each_key do |db_name|
          authenticate_to_db(db_name, db_pass)
          dump_collections(@db_connection.db(db_name))
        end
      end

      @solr.commit
    end

    # Dump all collections of the database (with the exception of system.* collections) to
    # Solr for indexing.
    #
    # @param db [Mongo::DB] The database instance to dump.
    def dump_collections(db)
      db.collection_names.each do |collection_name|
        unless collection_name =~ SPECIAL_COLLECTION_NAME_PATTERN then
          @logger.info "dumping #{db.name}.#{collection_name}..."

          db.collection(collection_name).find().each do |doc|
            @solr.add(DocumentTransform.translate_doc(doc))
          end
        end
      end
    end

    # Synchronize the contents of the database to Solr by applying the operations
    # in the oplog.
    #
    # @param oplog_doc_entries [Array<Object>] An array of Mongo documents containing
    #   the oplog entries to apply.
    def update_solr(oplog_doc_entries)
      update_list = {}

      oplog_doc_entries.each do |oplog_entry|
        namespace = oplog_entry["ns"]
        doc = oplog_entry["o"]

        case oplog_entry["op"]
        when "i" then
          @logger.info "adding #{doc.inspect}"
          @solr.add(DocumentTransform.translate_doc(doc))

        when "u" then
          # Batch the documents that needs a new update to minimize DB roundtrips.
          # The reason for querying the DB is because there is no update operation for Solr
          # (and also no way to modify a field), only replacing an existing entry with a new
          # one. Since the oplog only contains the diff operation, we need to fetch the 
          # latest content from the DB.
          to_update = update_list[namespace] ||= Set.new
          to_update << oplog_entry["o2"]["_id"]

        when "d" then
          # Remove entry in the update_list to avoid it from magically reappearing (from Solr
          # point of view) after deletion.
          to_update = update_list[namespace] ||= Set.new
          id = oplog_entry["o"]["_id"]
          to_update.delete(id)

          @logger.info "deleting #{doc.inspect}"
          @solr.delete_by_id id

        when "n" then
          # NOOP: do nothing

        else
          @logger.error "Unrecognized operation in oplog entry: #{oplog_entry.inspect}"
        end
      end

      update_list.each do |namespace, id_list|
        ns_split = namespace.split(".")
        database = ns_split.first
        collection = ns_split[1]

        to_update = @db_connection.db(database).collection(collection).
          find({"_id" => {"$in" => id_list.to_a}})

        to_update.each do |doc|
          @logger.info "updating #{doc.inspect}"
          @solr.add(DocumentTransform.translate_doc(doc))
        end
      end

      @solr.commit
    end

    # Helper method for authenticating to a database.
    #
    # @param db_name [String] The name of the database to authenticate
    # @param db_pass [Hash] @see #sync
    #
    # @return [Boolean] true if db_pass contains authentication information for db_name
    def authenticate_to_db(db_name, db_pass)
      auth = db_pass[db_name]
      
      if auth.nil? then
        false
      else
        @db_connection.db(db_name).authenticate(auth[:user], auth[:pwd], true)
        true
      end
    end

    # Helper method for determining whether to apply the oplog entry changes to Solr.
    #
    # @param namespace [String] The ns field in the oplog entry.
    #
    # @return [Boolean] true if the oplog entry should be skipped.
    def filter_entry?(namespace)
      split = namespace.split(".")
      db_name = split.first

      if db_name =~ SPECIAL_PURPOSE_MONGO_DB_NAME_PATTERN then
        return true
      else
        split.delete_at 0
        collection_name = split.join(".")
        return !(collection_name =~ SPECIAL_COLLECTION_NAME_PATTERN).nil?
      end
    end

    # @return [Mongo::Collection] the oplog collection
    #
    # @raise [OplogException]
    def get_oplog_collection
      oplog_coll = nil
      oplog_collection_name = case @mode
                              when :master_slave then MASTER_SLAVE_OPLOG_COLL_NAME
                              when :repl_set then REPL_SET_OPLOG_COLL_NAME
                              else ""
                              end

      oplog_db = @db_connection.db("local")

      if oplog_collection_name.empty? then
        # Try to figure out which collection exists in the database
        candidate_coll_names = []

        begin
          @mode = :master_slave
          get_oplog_collection
        rescue OplogException
          # Do nothing
        else
          candidate_coll_names << MASTER_SLAVE_OPLOG_COLL_NAME
        end

        begin
          @mode = :repl_set
          get_oplog_collection
        rescue OplogException
          # Do nothing
        else
          candidate_coll_names << REPL_SET_OPLOG_COLL_NAME
        end

        if candidate_coll_names.empty? then
          raise OplogException, OPLOG_NOT_FOUND_MSG
        elsif candidate_coll_names.size > 1 then
          raise OplogException, OPLOG_AMBIGUOUS_MSG
        else
          oplog_coll = oplog_db.collection(candidate_coll_names.first)
        end
      else
        begin
          oplog_db.validate_collection(oplog_collection_name)
        rescue Mongo::MongoDBError
          raise OplogException, OPLOG_NOT_FOUND_MSG
        end

        oplog_coll = oplog_db.collection(oplog_collection_name)
      end

      return oplog_coll
    end
  end
end

