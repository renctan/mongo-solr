require "rubygems"
require "rsolr"
require "mongo"
require "set"
require_relative "document_transform"
require_relative "exception"

module MongoSolr
  # A simple class utility for indexing an entire MongoDB database contents (excluding
  # administrative collections) to Solr.
  class SolrSynchronizer
    MONGO_DEFAULT_PORT = 27017

    # Create a synchronizer instance.
    #
    # @param solr [RSolr::Client] The client to the Solr server to populate database documents.
    # @param mongo_connection [Mongo::Connection] The connection to the database to synchronize.
    # @param mode [Symbol] Mode of the MongoDB server connected to. Accepted symbols - :repl_set,
    #   :master_slave
    #
    # @raise [OplogNotFoundException]
    #
    # Example:
    #  mongo = Mongo::Connection.new("localhost", 27017)
    #  solr_client = RSolr.connect(:url => "http://localhost:8983/solr")
    #  solr = MongoSolr::SolrSynchronizer.new(solr_client, mongo, :master_slave)
    def initialize(solr, mongo_connection, mode)
      @solr = solr
      @db_connection = mongo_connection
      oplog_db = @db_connection.db("local")

      oplog_collection_name = case mode
                              when :master_slave then "oplog.$main"
                              when :repl_set then "oplog.rs"
                              else ""
                              end

      begin
        oplog_db.validate_collection(oplog_collection_name)
      rescue
        raise OplogNotFoundException, "Cannot find oplog collection. Make sure that " +
          "you are connected to a server running on master/slave or replica set configuration."
      end

      @oplog_collection = oplog_db.collection(oplog_collection_name)
    end

    # Continuously synchronizes the database contents with Solr. Please note that this is a
    # blocking call that contains an infinite loop.
    #
    # @param interval [number] (1) The interval in seconds to wait before checking for new updates
    # @param block [Proc(mode, doc_count)] A procedure that will be called during certain conditions:
    #   1. block(:finished_dumping, 0) will be called after dumping the contents of the database.
    #   2. block(:sync, doc_count) will be called everytime Solr is updated from the oplog.
    #      doc_count contains the number of docs synched with Solr so far.
    def sync(interval = 1, &block)
      cursor = Mongo::Cursor.new(@oplog_collection,
                                 { :tailable => true, :selector => {"op" => {"$ne" => "n"} } })

      # Go to the tail of cursor. Must find a better way moving the cursor to the latest entry.
      while cursor.next_document do
        # Do nothing
      end

      dump_db_contents
      yield :finished_dumping, 0

      doc_count = 0
      loop do
        doc_batch = []

        while doc = cursor.next_document do
          doc_batch << doc
          doc_count += 1
        end

        update_solr(doc_batch)
        yield :sync, doc_count

        sleep interval
      end
    end

    ############################################################################
    private

    # Dump the all contents of the MongoDB server to be indexed to Solr.
    # Assumption: no authentication is required to access the databases.
    def dump_db_contents
      @db_connection.database_names.each do |db_name|
        unless db_name == "local" or db_name == "admin" then
          db = @db_connection.db(db_name)

          db.collection_names.each do |collection_name|
            unless collection_name == "system.indexes" then
#              puts "dumping #{db_name}.#{collection_name}..."

              db.collection(collection_name).find().each do |doc|
                @solr.add(DocumentTransform.translate_doc(doc))
              end
            end
          end
        end
      end

      @solr.commit
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
#          puts "adding #{doc.inspect}"
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

          @solr.delete_by_id id

        when "n" then
          # NOOP: do nothing

        else
          puts "ERROR: unrecognized operation in oplog entry: #{oplog_entry.inspect}"
        end
      end

      update_list.each do |namespace, id_list|
        ns_split = namespace.split(".")
        database = ns_split.first
        collection = ns_split[1]

        to_update = @db_connection.db(database).collection(collection).
          find({"_id" => {"$in" => id_list.to_a}})
        to_update.each { @solr.add(DocumentTransform.translate_doc(doc)) }
      end

      @solr.commit
    end
  end
end

