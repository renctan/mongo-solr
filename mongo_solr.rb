require "rubygems"
require "rsolr"
require "mongo"
require "set"

module MongoSolr
  # TODO:
  # - Deal with embedded documents
  # - MongoDB db & collection namespace to Solr namespace translation
  class SolrSynchronizer
    MONGO_DEFAULT_PORT = 27017

    # @param solr_server_loc [String] location of the Solr server to populate the database documents
    # @param mongoloc [String] location of the mongod
    # @param options [?Hash] additional options. List of the recognized keys:
    #   :mongo_port [number] => the port to use when connecting to the mongo server, uses
    #     MONGO_DEFAULT_PORT when not specified
    #   :is_master_slave [boolean] => true if the mongo server is running on master/slave mode
    #  
    # Examples:
    #  solr = MongoSolr::SolrSynchronizer.new("http://localhost:8983/solr", "localhost")
    #  solr = MongoSolr::SolrSynchronizer.new("http://localhost:8983/solr", "localhost",
    #    { :mongo_port => 27017, is_master_slave => false })
    def initialize(solr_server_loc, mongo_loc, options)
      @solr = RSolr.connect :url => solr_server_loc

      if options.nil? then
        mongo_port = MONGO_DEFAULT_PORT
        is_master_slave = false
      else
        mongo_port = options[:mongo_port] || MONGO_DEFAULT_PORT
        is_master_slave = options[:is_master_slave] || false
      end

      if is_master_slave then
        oplog_db_name = "local"
        oplog_collection = "oplog.$main"
      else
        # TODO: for replica sets
        oplog_db_name = "local"
        oplog_collection = "oplog.$main"
      end

      @db = Mongo::Connection.new(mongo_loc, mongo_port)
      @oplog_collection = @db.db(oplog_db_name).collection(oplog_collection)
    end

    # Synchronizes the database contents with Solr
    #
    # @param interval [number] The interval in seconds to wait before checking for new updates
    def sync(interval = 1)
      dump_db_contents

      cursor = Mongo::Cursor.new(@oplog_collection, :tailable => true)

      loop do
        doc_batch = []

        while doc = cursor.next_document do
          doc_batch << doc
        end

        update_solr(doc_batch)
        sleep interval
      end
    end

    ############################################################################
    private
    def dump_db_contents
      # TODO: implement
    end

    def update_solr(oplog_doc_entries)
      update_list = {}

      oplog_doc_entries.each do |oplog_entry|
        namespace = oplog_entry["ns"]
        doc = oplog_entry["o"]

        case oplog_entry["op"]
        when "i" then
          puts "adding #{doc.inspect}"
          @solr.add(doc)

        when "u" then
          # Batch the documents that needs a new update to minimize DB roundtrips
          to_update = update_list[namespace] ||= Set.new
          to_update << oplog_entry["o2"]["_id"]

        when "d" then
          # Remove entry in the update_list since there is no update operation for Solr,
          # only replacing an existing entry with a new one
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
        id_list.to_a
      end

      @solr.commit
    end
  end
end

