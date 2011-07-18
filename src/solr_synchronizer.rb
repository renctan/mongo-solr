require "rubygems"
require "mongo"
require "set"
require "logger"

require_relative "document_transform"
require_relative "exception"
require_relative "synchronized_hash"
require_relative "solr_retry_decorator"

module MongoSolr
  # A simple class utility for indexing an entire MongoDB database contents (excluding
  # administrative collections) to Solr.
  class SolrSynchronizer
    # [Logger] Set the logger to be used to output. Defaults to STDOUT.
    attr_writer :logger

    # Creates a synchronizer instance.
    #
    # @param solr [RSolr::Client] The client to the Solr server to populate database documents.
    # @param mongo_connection [Mongo::Connection] The connection to the database to synchronize.
    # @param mode [Symbol] Mode of the MongoDB server connected to. Accepted symbols - :repl_set,
    #   :master_slave, :auto
    # @param db_set [Hash<String, Set<String> >] ({}) The set of databases and their
    #   collections to index to Solr. The key should contain the database name in
    #   String and the value should be an array that contains the names of collections.
    # @option opt [Logger] :logger The object to use for logging. The default logger outputs
    #   to stdout.
    # @option opt [String] :name ("") A string label that will be prefixed to all log outputs.
    # @option opt [number] :interval (1) The interval in seconds to wait before checking for
    #    new updates in the database
    # @option opt [number] :err_retry_interval (1) The interval in seconds to retry again
    #    after encountering an error in the Solr server or MongoDB instance.
    #
    # Example:
    #  mongo = Mongo::Connection.new("localhost", 27017)
    #  solr_client = RSolr.connect(:url => "http://localhost:8983/solr")
    #  solr = MongoSolr::SolrSynchronizer.new(solr_client, mongo, :master_slave)
    def initialize(solr, mongo_connection, mode, db_set = {}, opt = {})
      @db_connection = mongo_connection
      @mode = mode
      @logger = opt[:logger] || Logger.new(STDOUT)
      @name = opt[:name] || ""
      @update_interval = opt[:interval] || 1
      @err_retry_interval = opt[:err_retry_interval] || 1

      @solr = SolrRetryDecorator.new(solr, @err_retry_interval, @logger)

      # The set of collections to listen to for updates in the oplogs.
      @db_set = MongoSolr::SynchronizedHash.new(db_set)

      # The oplog data for collections that are still in the process of dumping.
      @oplog_backlog = MongoSolr::SynchronizedHash.new

      # Locks should be taken in this order to avoid deadlock
      @stop_mutex = Mutex.new
      @synching_mutex = Mutex.new
      # implicit db_set mutex
      # implicit synching_set mutex

      # True to tell the sync thread to stop.
      # protected by @stop_mutex
      @stop = false

      # True if this object is currently performing a sync with the database
      # protected by @synching_mutex
      @is_synching = false 
    end

    # Stop the sync operation
    def stop!
      # Lock usage:
      # 1. @stop_mutex

      @stop_mutex.synchronize { @stop = true }
    end

    # Replaces the current set of collection to listen with a new one.
    #
    # @param new_db_set [Hash<String, Set<String> >] The set of databases and their
    #   collections to index to Solr. The key should contain the database name in
    #   String and the value should be an array that contains the names of collections.
    #   An empty hash means that everything will be indexed.
    # @param wait [Boolean] Waits until the newly added sets are not in a backlogged state
    #   before returning if set to true. Defaults to false.
    def update_db_set(new_db_set, wait = false)
      # Lock usage:
      # 1. @is_synching->@db_set->add_collection_wo_lock()

      @synching_mutex.synchronize do
        @db_set.use do |db_set|
          new_db_set.each do |db_name, collection|
            add_collection_wo_lock(db_set, @is_synching, db_name, collection, wait)
          end

          db_set.clear
          db_set.merge!(new_db_set)
        end
      end
    end

    # Adds a new collection to index.
    #
    # @param db_name [String] Name of the database the collection belongs to.
    # @param collection [String|Enumerable] Name of the collection(s) to add.
    # @param wait [Boolean] Waits until the newly added sets are not in a backlogged state
    #   before returning if set to true. Defaults to false.
    # @param block[Proc] For debugging/testing only. @see #add_collection_wo_lock
    def add_collection(db_name, collection, wait = false, &block)
      # Lock usage:
      # 1. @is_synching->@db_set->add_collection_wo_lock()      

      @synching_mutex.synchronize do
        @db_set.use do |db_set|
          add_collection_wo_lock(db_set, @is_synching, db_name, collection, wait, &block)
        end
      end
    end

    # Continuously synchronizes the database contents with Solr. Please note that this is a
    # blocking call that contains an infinite loop. This method assumes that the databases
    # are already authenticated.
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
      # Lock usage:
      # 1. @stop_mutex->@synching_mutex
      # 2. get_db_set_snapshot()
      # 3. insert_to_backlog()
      # 4. stop_synching?()

      @stop_mutex.synchronize do
        @synching_mutex.synchronize do
          if @is_synching then
            return
          else
            @stop = false
            @is_synching = true
          end
        end
      end

      cursor = get_oplog_cursor(get_last_oplog_timestamp)

      db_set_snapshot = get_db_set_snapshot
      dump_db_contents(db_set_snapshot)
      yield :finished_dumping, 0 if block_given?

      doc_count = 0

      loop do
        return if stop_synching?
        doc_batch = []
        db_set_snapshot = get_db_set_snapshot

        while doc = cursor.next_document do
          if insert_to_backlog(doc) then
            # Do nothing
          elsif filter_entry?(db_set_snapshot, doc["ns"]) then
            @logger.debug "#{@name}: skipped oplog: #{doc}"
          else
            doc_batch << doc
            doc_count += 1
          end

          return if stop_synching?
        end

        update_solr(doc_batch)
        yield :sync, doc_count if block_given?

        sleep @update_interval unless @update_interval.zero?
      end
    end

    # Gets the current snapshot of the db_set.
    #
    # @return [Hash<String, Enumerable<String> >] a hash for the set of collections to index.
    #   The key contains the name of the database while the value contains an array of
    #   collection names.
    def get_db_set_snapshot
      # Lock usage: @db_set
      return @db_set.use { |db_set| db_set.clone }
    end

    alias_method :db_set, :get_db_set_snapshot

    ############################################################################
    private
    SPECIAL_COLLECTION_NAME_PATTERN = /^system\./
    MASTER_SLAVE_OPLOG_COLL_NAME = "oplog.$main"
    REPL_SET_OPLOG_COLL_NAME = "oplog.rs"
    OPLOG_NOT_FOUND_MSG = "Cannot find oplog collection. Make sure that " +
      "you are connected to a server running on master/slave or replica set configuration."
    OPLOG_AMBIGUOUS_MSG = "Cannot determine which oplog to use. Please specify " +
      "the appropriate mode."

    # Dumps the contents of the MongoDB server to be indexed to Solr.
    #
    # @param db_set [Hash<String, Array<String> >] a hash which contains the set of
    #   collections to index. The key contains the name of the database while the value
    #   contains an array of collection names.
    def dump_db_contents(db_set)
      db_set.each do |db_name, collections|
        db = @db_connection.db(db_name)
        collections.each { |coll| dump_collection(db, coll) }
      end

      @solr.commit
    end

    # Dumps all collections of the database (with the exception of system.* collections) to
    # Solr for indexing without committing.
    #
    # @param db [Mongo::DB] The database instance to dump.
    def dump_collections(db)
      db.collection_names.each do |collection_name|
        unless collection_name =~ SPECIAL_COLLECTION_NAME_PATTERN then
          dump_collection(db, collection_name)
        end
      end
    end

    # Dumps the contents of a collection to Solr without committing.
    #
    # @param db [Mongo::Database] The database of the collection.
    # @param collection_name [String] The name of the collection.
    def dump_collection(db, collection_name)
      @logger.info "#{@name}: dumping #{db.name}.#{collection_name}..."

      db.collection(collection_name).find().each do |doc|
        @solr.add(DocumentTransform.translate_doc(doc))
      end
    end

    # Synchronizes the contents of the database to Solr by applying the operations
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
          @logger.info "#{@name}: adding #{doc.inspect}"
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

          @logger.info "#{@name}: deleting #{doc.inspect}"
          @solr.delete_by_id id

        when "n" then
          # NOOP: do nothing

        else
          @logger.error "#{@name}: Unrecognized operation in oplog entry: #{oplog_entry.inspect}"
        end
      end

      update_list.each do |namespace, id_list|
        ns_split = namespace.split(".")
        database = ns_split.first
        collection = ns_split[1]

        to_update = @db_connection.db(database).collection(collection).
          find({"_id" => {"$in" => id_list.to_a}})

        to_update.each do |doc|
          @logger.info "#{@name}: updating #{doc.inspect}"
          @solr.add(DocumentTransform.translate_doc(doc))
        end
      end

      @solr.commit
    end

    # Helper method for determining whether to apply the oplog entry changes to Solr.
    #
    # @param db_set [Hash<String, Array<String> >] a hash which contains the set of
    #   collections to index. The key contains the name of the database while the value
    #   contains an array of collection names.
    # @param namespace [String] The ns field in the oplog entry.
    #
    # @return [Boolean] true if the oplog entry should be skipped.
    def filter_entry?(db_set, namespace)
      split = namespace.split(".")
      db_name = split.first

      split.delete_at 0
      collection_name = split.join(".")

      do_skip = true

      if db_set.has_key?(db_name) then
        do_skip = !db_set[db_name].include?(collection_name)
      end

      return do_skip
    end
    
    # @param mode [Symbol] The mode of which the database server is running on. Recognized
    #   symbols: :master_slave, :repl_set, :auto
    #
    # @return [Mongo::Collection] the oplog collection
    #
    # @raise [OplogException]
    def get_oplog_collection(mode)
      oplog_coll = nil
      oplog_collection_name = case mode
                              when :master_slave then MASTER_SLAVE_OPLOG_COLL_NAME
                              when :repl_set then REPL_SET_OPLOG_COLL_NAME
                              else ""
                              end

      oplog_db = @db_connection.db("local")

      if oplog_collection_name.empty? then
        # Try to figure out which collection exists in the database
        candidate_coll = []

        begin
          candidate_coll << get_oplog_collection(:master_slave)
        rescue OplogException
          # Do nothing
        end

        begin
          candidate_coll << get_oplog_collection(:repl_set)
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
        begin
          oplog_db.validate_collection(oplog_collection_name)
        rescue Mongo::MongoDBError
          raise OplogException, OPLOG_NOT_FOUND_MSG
        end

        oplog_coll = oplog_db.collection(oplog_collection_name)
      end

      return oplog_coll
    end

    # Adds a collection without using a lock. Caller should be holding the lock for
    # @synching_mutex and @db_set while calling this method.
    #
    # @param db_set [Hash] The hash inside the SynchronizedHash wrapper.
    # @param is_synching [Boolean]
    # @param db_name [String] The name of the database
    # @param collection [String|Enumerable] Name of the collection(s) to add.
    # @param wait [Boolean] Waits until the newly added sets are not in a backlogged state
    #   before returning if set to true.
    # @param &block [Proc(mode, backlog)] Extra block parameter for debugging and testing.
    #   The mode can be :finished_dumping or :depleted_backlog, which describes on which
    #   stage it is running on. The backlog paramereter is the array of backlogged oplog
    #   This block will be called repeatedly during the :finished_dumping stage until the
    #   block returns (break) a false value
    def add_collection_wo_lock(db_set, is_synching, db_name, collection, wait, &block)
      # Lock usage:
      # 1. @oplog_backlog
      # 2. Spawns a thread that uses @oplog_backlog

      db_set_was_empty_before = db_set.empty?

      if db_set.has_key? db_name then
        current_collection = db_set[db_name]
      else
        current_collection = Set.new
        db_set[db_name] = current_collection
      end

      if collection.is_a? Enumerable then
        new_collection = Set.new(collection)
      else
        new_collection = Set.new([collection])
      end

      diff = new_collection - current_collection
      current_collection.merge(diff)

      if is_synching and not db_set_was_empty_before then
        db = @db_connection.db(db_name)

        diff.each do |coll|
          oplog_ns = "#{db_name}.#{coll}"
          @oplog_backlog[oplog_ns] = []

          backlog_sync_thread = Thread.start do
            dump_collection(db, coll)

            # For testing/debugging only
            if block_given? then
              block_val = true
              while block_val do
                block_val = yield :finished_dumping, @oplog_backlog[oplog_ns]
                # Do nothing
              end
            end
            
            # Consume all oplogs that have accumulated while performing the dump and
            # while performing this task.
            @oplog_backlog.use do |backlog, mutex|
              until backlog[oplog_ns].empty?
                oplog_entries = backlog[oplog_ns]
                backlog[oplog_ns] = []

                mutex.unlock
                update_solr(oplog_entries)
                mutex.lock
              end

              backlog.delete(oplog_ns)
            end

            yield :depleted_backlog if block_given?
          end

          backlog_sync_thread.join if wait
        end
      end
    end

    # Inserts an oplog entry to the backlog if there is still a thread dumping the collection
    # for the given namespace.
    #
    # @param oplog_entry [Object] The document 
    #
    # @return true when the entry was inserted to the backlog
    def insert_to_backlog(oplog_entry)
      ns = oplog_entry["ns"]
      inserted = false

      @oplog_backlog.use do |backlog|
        if backlog.has_key? ns then
          backlog[ns] << oplog_entry
          inserted = true
        end
      end

      return inserted
    end

    # @return true when the sync thread needs to stop.
    def stop_synching?
      # Lock usage:
      # 1. @stop_mutex->@synching_mutex

      do_stop = false

      @stop_mutex.synchronize do
        if @stop then
          @synching_mutex.synchronize { @is_synching = false }
          do_stop = true
        end
      end

      @logger.info "#{@name}: Stopping sync..." if do_stop
      return do_stop
    end

    # Gets the timestamp of the latest entry in the oplog
    #
    # @return [BSON::Timestamp] the timestamp object.
    def get_last_oplog_timestamp
      cur = get_oplog_collection(@mode).find({}, :sort => ["$natural", :desc],
                                             :limit => 1)
      cur.next["ts"]
    end

    # Acquire a tailable cursor on the oplog with time greater than the given timestamp.
    #
    # @param timestamp [BSON::Timestamp] The timestamp object.
    #
    # @return [Mongo::Cursor] the tailable cursor
    def get_oplog_cursor(timestamp)
      Mongo::Cursor.new(get_oplog_collection(@mode),
                        { :tailable => true,
                          :selector => {"op" => {"$ne" => "n"},
                            "ts" => {"$gt" => timestamp } }
                        })
    end
  end
end

