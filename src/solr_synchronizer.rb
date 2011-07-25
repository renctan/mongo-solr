require "rubygems"
require "mongo"
require "set"
require "logger"

require_relative "document_transform"
require_relative "exception"
require_relative "synchronized_hash"
require_relative "solr_retry_decorator"
require_relative "util"

module MongoSolr
  # A simple class utility for indexing an entire MongoDB database contents (excluding
  # administrative collections) to Solr.
  class SolrSynchronizer
    # Creates a synchronizer instance.
    #
    # @param solr [RSolr::Client] The client to the Solr server to populate database documents.
    # @param mongo_connection [Mongo::Connection] The connection to the database to synchronize.
    # @param config_writer [MongoSolr::ConfigWriter] The object that can be used to update
    #   the configuration.
    #
    # @option opt [Symbol] :mode (:auto) Mode of the MongoDB server connected to. Recognized 
    #   symbols - :repl_set, :master_slave, :auto
    # @option opt [Hash<String, Set<String> >] :db_set ({}) The set of databases and their
    #   collections to index to Solr. @see #update_db_set
    # @option opt [Logger] :logger The object to use for logging. The default logger outputs
    #   to stdout.
    # @option opt [String] :name ("") A string label that will be prefixed to all log outputs.
    # @option opt [number] :interval (0) The interval in seconds to wait before checking for
    #    new updates in the database
    # @option opt [number] :err_retry_interval (1) The interval in seconds to retry again
    #    after encountering an error in the Solr server or MongoDB instance.
    #
    # Note: This object assumes that the solr and mongo_connection params are valid. As a
    #   a consequence, it will keep on retrying whenever an exception on these objects
    #   occured.
    #
    # Example:
    #  mongo = Mongo::Connection.new("localhost", 27017)
    #  solr_client = RSolr.connect(:url => "http://localhost:8983/solr")
    #  solr = MongoSolr::SolrSynchronizer.new(solr_client, mongo, :master_slave)
    def initialize(solr, mongo_connection, config_writer, opt = {})
      @db_connection = mongo_connection
      @mode = opt[:mode] || :auto
      @logger = opt[:logger] || Logger.new(STDOUT)
      @name = opt[:name] || ""
      @update_interval = opt[:interval] || 0
      @err_retry_interval = opt[:err_retry_interval] || 1
      @config_writer = config_writer
      @last_update = nil

      @solr = SolrRetryDecorator.new(solr, @err_retry_interval, @logger)

      # The set of collections to listen to for updates in the oplogs.
      db_set = opt[:db_set] || {}
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
    # @param new_db_set [Hash<String, Array<Hash> >] The set of databases and their
    #   collections to index to Solr. The key should contain the database name in
    #   String and the value should be an array that contains the collections.
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
    # @option opt [MongoSolr::CheckpointData] :checkpt (nil) This will be used to
    #   continue from a previous session if given.
    # @option opt [Boolean] :wait (false) True means wait for some asynchronous calls to
    #   actually terminate before proceeding. Useful for testing.
    # @param block [Proc(mode, doc_count)] An optional  procedure that will be called during
    #   certain conditions:
    #
    #   1. block(:finished_dumping, 0) will be called after dumping the contents of the database.
    #   2. block(:sync, doc_count) will be called everytime Solr is updated from the oplog.
    #      doc_count contains the number of docs synched with Solr so far.
    #   3. block(:excep, doc_count) will be called everytime an exception occured while trying
    #      to update Solr.
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

      checkpoint_data = opt[:checkpt]
      wait = opt[:wait] || false

      cursor = init_sync(checkpoint_data, wait)

      yield :finished_dumping, 0 if block_given?

      loop do
        return if stop_synching?
        doc_batch = []
        db_set_snapshot = get_db_set_snapshot
        doc_count = 0 # count for the current batch
        cursor_exception_occured = false

        loop do
          begin
            doc = cursor.next_document
          rescue Mongo::OperationFailure
            raise "Sync cursor is too state: Cannot catch up with the update rate." + 
              "Please perform a manual dump."
          rescue => e
            @logger.error "#{@name}: #{e.message}"
            cursor_exception_occured = true
            yield :excep, doc_count if block_given?
            break
          end

          if doc.nil? then
            break
          else
            if insert_to_backlog(doc) then
              # Do nothing
            elsif filter_entry?(db_set_snapshot, doc["ns"]) then
              @logger.debug "#{@name}: skipped oplog: #{doc}"
            else
              doc_batch << doc
              doc_count += 1
            end

            @last_update = doc["ts"]
          end

          return if stop_synching?
          break if doc_count > OPLOG_BATCH_SIZE
        end

        update_solr(doc_batch, true)

        yield :sync, doc_count if block_given?

        sleep @update_interval unless @update_interval.zero?

        # Setting of cursor was deferred until here to do work with Solr while
        # waiting for connection to Mongo to recover.
        if cursor_exception_occured then
          cursor = retry_until_ok { get_last_update_cursor }

          if cursor.nil? then
            # TODO: Too stale. Raise?
            # cursor = retry_until_ok { get_oplog_cursor(get_last_oplog_timestamp) }
            # dump_db_contents(db_set_snapshot)
          end
        end
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
    OPLOG_BATCH_SIZE = 200

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

      retry_until_ok do
        db.collection(collection_name).find().each do |doc|
          @solr.add(DocumentTransform.translate_doc(doc))
          # Do not update commit timestamp since the stream of data from the database
          # is not guaranteed to be sequential in time.
        end
      end
    end

    # Synchronizes the contents of the database to Solr by applying the operations
    # in the oplog.
    #
    # @param oplog_doc_entries [Array<Object>] An array of Mongo documents containing
    #   the oplog entries to apply.
    # @param do_timestamp_commit [Boolean] (false) Record the timestamp when commiting to
    #   Solr
    def update_solr(oplog_doc_entries, do_timestamp_commit = false)
      update_list = {}
      timestamp = nil

      oplog_doc_entries.each do |oplog_entry|
        namespace = oplog_entry["ns"]
        doc = oplog_entry["o"]
        timestamp = oplog_entry["ts"]

        case oplog_entry["op"]
        when "i" then
          @logger.info "#{@name}: adding #{doc.inspect}"
          @solr.add(DocumentTransform.translate_doc(doc))
          @config_writer.update_timestamp(namespace, timestamp) unless timestamp.nil?

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
          @config_writer.update_timestamp(namespace, timestamp) unless timestamp.nil?

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

        retry_until_ok do
          to_update = @db_connection.db(database).collection(collection).
            find({"_id" => {"$in" => id_list.to_a}})

          to_update.each do |doc|
            @logger.info "#{@name}: updating #{doc.inspect}"
            @solr.add(DocumentTransform.translate_doc(doc))
            # Use the last timestamp from oplog_doc_entries.
            @config_writer.update_timestamp(namespace, timestamp) unless timestamp.nil?

            # Remove from the set so there will be less documents to update
            # when an exception occurs and this block is executed again
            id_list.delete doc["_id"]
          end
        end
      end

      @solr.commit

      if do_timestamp_commit then
        @config_writer.update_commit_timestamp(timestamp) 
        @last_update = timestamp
      end
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

      if is_synching then
        diff.each do |coll|
          dump_and_sync(db_name, coll, wait, &block)
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
      cur = retry_until_ok do
        get_oplog_collection(@mode).find({}, :sort => ["$natural", :desc],
                                         :limit => 1)
      end

      cur.next["ts"]
    end

    # Acquire a tailable cursor on the oplog with time greater and equal to the given
    # timestamp.
    #
    # @param timestamp [BSON::Timestamp] The timestamp object.
    #
    # @return [Mongo::Cursor] the tailable cursor
    def get_oplog_cursor(timestamp)
      Mongo::Cursor.new(get_oplog_collection(@mode),
                        { :tailable => true,
                          :selector => {"op" => {"$ne" => "n"},
                            "ts" => {"$gte" => timestamp } }
                        })
    end

    # Convenience method for reattempting an operation until the execution is free of
    # exception.
    #
    # @param block [Proc] The procedure to perform. The procedure should be indepotent.
    #
    # @return [Object] the return value of the block.
    def retry_until_ok(&block)
      begin
        yield block
      rescue => e
        @logger.error "#{@name}: #{e.message}"
        sleep @err_retry_interval
        retry
      end
    end

    # @return [Mongo::Cursor] the cursor that points to the next oplog entry to the last
    #   update. nil if the last known update timestamp is too old.
    def get_last_update_cursor
      ret = nil

      unless @last_update.nil? then
        cursor = get_oplog_cursor(@last_update)
        doc = cursor.next_document

        if @last_update == doc["ts"] then
          ret = cursor
        else
          @logger.warn("#{@name}: Last update (#{@last_update.inspect}) is too old. " +
                       "Oldest oplog ts: #{doc["ts"].inspect}")
        end
      end

      return ret
    end

    # Creates a new thread to dump the contents of the given collection to Solr and
    # tries to get in sync with the main thread by consuming the backlogs inserted
    # by the main thread for the given namespace.
    #
    # @param db_name [String] The name of the database.
    # @param coll_name [String] The name of the collection.
    # @param wait [Boolean] Waits for the thread to finish before returning.
    # @param block [Param] @see #add_collection_wo_lock
    def dump_and_sync(db_name, coll_name, wait = false, &block)
      oplog_ns = "#{db_name}.#{coll_name}"
      @oplog_backlog[oplog_ns] = []

      db = retry_until_ok { @db_connection.db(db_name) }

      backlog_sync_thread = Thread.start do
        dump_collection(db, coll_name)
        @solr.commit

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

    # Sets the state of this object to reflect the state of a given checkpoint.
    #
    # @param checkpoint_data [MongoSolr::CheckpointData] The checkpoint data.
    # @param wait [Boolean] Waits for the dumps to actually finish before proceeding if
    #   true. Dumps only happen when the checkpoint is too old/stale.
    def restore_from_checkpoint(checkpoint_data, wait = false)
      last_ts = checkpoint_data.commit_ts
      oplog_coll = get_oplog_collection(@mode)
      docs_to_update = []

      # Make sure that everything before the last commit timestamp is already indexed
      # to Solr. Possible cases of having these kind of entries include:
      #
      # 1. A newly added collection/field to index was not able to completely consume
      #    its backlog.
      checkpoint_data.each do |ns, ts|
        split = ns.split(".")
        db_name = split.first

        split.delete_at 0
        coll_name = split.join(".")

        if ts.nil? then
          dump_and_sync(db_name, coll_name, wait)
        elsif Util.compare_bson_ts(ts, last_ts) == -1 then
          doc_results = oplog_coll.find({ "ns" => ns,
                                          "ts" => {"$gte" => ts, "$lte" => last_ts} }).to_a
          if (ts != doc_results.shift["ts"]) then
            # Timestamp is too old that the oplog entry for it is not available anymore
            dump_and_sync(db_name, coll_name, wait)
          else
            docs_to_update.concat(doc_results)
          end
        else
          # No new ops since last update on Solr. Advance to the global commit timestamp.
          @config_writer.update_timestamp(ns, last_ts)
        end
      end

      update_solr(docs_to_update)
    end

    # Convenience method for initializing the cursor for sync method. Also performs a
    # database dump when this was called for the first time.
    #
    # @param checkpoint_data [MongoSolr::CheckpointData] @see #sync
    # @param wait [Boolean] @see #sync
    #
    # @return [Mongo::Cursor] the cursor to the oplog entry, pointing to the next oplog
    #   entry to process.
    def init_sync(checkpoint_data, wait)
      cursor = nil

      unless checkpoint_data.nil? then
        restore_from_checkpoint(checkpoint_data, wait)
        @last_update = checkpoint_data.commit_ts
        cursor = retry_until_ok { get_last_update_cursor }
        perform_full_dump if cursor.nil?
      end

      if @last_update.nil? then
        cursor = perform_full_dump
      else
        cursor = retry_until_ok { get_last_update_cursor }
        perform_full_dump if cursor.nil?
      end

      return cursor
    end

    # Performs a dump of the database (filtered by the current db_set) and returns the
    # cursor to the oplog that points to the latest entry just before the dump is
    # performed
    #
    # @return [Mongo::Cursor] the oplog cursor
    def perform_full_dump
      cursor = retry_until_ok do
        @last_update = get_last_oplog_timestamp
        get_last_update_cursor
      end

      db_set_snapshot = get_db_set_snapshot
      dump_db_contents(db_set_snapshot)

      return cursor
    end
  end
end

