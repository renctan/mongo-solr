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
    # @option opt [Hash<String, Set<String> >] :ns_set ({}) The set of namespaces to index.
    #   The key contains the name of the namespace and the value contains the names of the
    #   fields. Empty set for fields means all fields will be indexed.
    # @option opt [Logger] :logger The object to use for logging. The default logger outputs
    #   to stdout.
    # @option opt [String] :name ("") A string label that will be prefixed to all log outputs.
    # @option opt [number] :interval (0) The interval in seconds to wait before checking for
    #   new updates in the database
    # @option opt [number] :err_retry_interval (1) The interval in seconds to retry again
    #   after encountering an error in the Solr server or MongoDB instance.
    # @option opt [Boolean] :auto_dump (false) If true, performs a full db dump when the
    #   oplog cursor gets too stale, otherwise, throws an exception.
    # @opt [CheckpointData] :checkpt (nil) This will be used to continue from a previous
    #   session if given.
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
      @auto_dump = opt[:auto_dump] || false
      @config_writer = config_writer

      @solr = SolrRetryDecorator.new(solr, @err_retry_interval, @logger)

      # The set of collections to listen to for updates in the oplogs.
      ns_set = opt[:ns_set] || {}      
      @ns_set = MongoSolr::SynchronizedHash.new(ns_set)

      # The oplog data for collections that are still in the process of dumping.
      @oplog_backlog = MongoSolr::SynchronizedHash.new

      # Locks should be taken in this order to avoid deadlock
      @stop_mutex = Mutex.new
      @synching_mutex = Mutex.new
      @checkpoint_mutex = Mutex.new

      # implicit ns_set mutex
      # implicit synching_set mutex

      # True to tell the sync thread to stop.
      # protected by @stop_mutex
      @stop = false

      # True if this object is currently performing a sync with the database
      # protected by @synching_mutex
      @is_synching = false 

      # protected by @checkpoint_mutex
      @checkpoint = opt[:checkpt]
    end

    # Stop the sync operation
    def stop!
      # Lock usage:
      # 1. @stop_mutex

      @stop_mutex.synchronize { @stop = true }
    end

    # Replaces the current set of collection to listen with a new one.
    #
    # @option opt [Hash<String, Set<String> >] :ns_set ({}) @see #new
    # @opt [CheckpointData] :checkpt (nil) This will be used to continue from a previous
    #   session if given.
    # @opt [Boolean] :wait (false) Waits until the newly added sets are not in a backlogged state
    #   before returning if set to true.
    # @param block [Proc] @see #dump_and_sync
    def update_config(opt = {}, &block)
      # Lock usage:
      # 1. @checkpoint_mutex
      # 2. @is_synching->@ns_set->add_collection_wo_lock()

      wait = opt[:wait] || false
      checkpoint_opt = opt[:checkpt]

      unless checkpoint_opt.nil? then
        @checkpoint_mutex.synchronize { @checkpoint = checkpoint_opt }
      end

      new_ns_set = opt[:ns_set]

      unless new_ns_set.nil? then
        @synching_mutex.synchronize do
          @ns_set.use do |ns_set, mutex|
            diff = new_ns_set.reject { |k, v| ns_set.has_key?(k) }
            add_namespace(diff, checkpoint_opt, @is_synching, wait, &block)

            @ns_set = SynchronizedHash.new(new_ns_set)
          end
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
    #   3. block(:excep, doc_count) will be called everytime an exception occured while trying
    #      to update Solr.
    #
    # @raise [OplogException]
    #
    # Example:
    # auth = { "users" => { :user => "root", :pwd => "root" },
    #          "admin" => { :user => "admin", :pwd => "" } }
    # solr.sync({ :db_pass => auth, :interval => 1 })
    def sync(&block)
      # Lock usage:
      # 1. @stop_mutex->@synching_mutex
      # 2. @checkpoint_mutex
      # 3. get_ns_set_snapshot()
      # 4. insert_to_backlog()
      # 5. stop_synching?()

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

      checkpoint = @checkpoint_mutex.synchronize do
        @checkpoint.clone unless @checkpoint.nil?
      end

      cursor = init_sync(checkpoint)
      last_timestamp = (checkpoint.nil? ? nil : checkpoint.commit_ts)

      yield :finished_dumping, 0 if block_given?

      loop do
        return if stop_synching?
        doc_batch = []
        ns_set_snapshot = get_ns_set_snapshot
        doc_count = 0 # count for the current batch
        cursor_exception_occured = false

        loop do
          begin
            doc = cursor.next_document
          rescue Mongo::OperationFailure
            if @auto_dump then
              cursor = perform_full_dump
            else
              raise StaleCursorException, STALE_CURSOR_MSG
            end
          rescue => e
            @logger.error "#{@name}: #{Util.get_full_exception_msg(e)}"
            cursor_exception_occured = true
            yield :excep, doc_count if block_given?
            break
          end

          if doc.nil? then
            break
          else
            if insert_to_backlog(doc) then
              # Do nothing
            elsif filter_entry?(ns_set_snapshot, doc["ns"]) then
              @logger.debug "#{@name}: skipped oplog: #{doc}"
            else
              doc_batch << doc
              doc_count += 1
            end

            last_timestamp = doc["ts"]
          end

          return if stop_synching?
          break if doc_count > OPLOG_BATCH_SIZE
        end

        update_solr(doc_batch, true) unless doc_batch.empty?

        yield :sync, doc_count if block_given?

        sleep @update_interval unless @update_interval.zero?

        # Setting of cursor was deferred until here to do work with Solr while
        # waiting for connection to Mongo to recover.
        if cursor_exception_occured then
          if last_timestamp.nil? then
            cursor = retry_until_ok do
              timestamp = get_last_oplog_timestamp
              get_oplog_cursor(timestamp)
            end
          else
            cursor = retry_until_ok { get_oplog_cursor_w_check(last_timestamp) }
          end

          if cursor.nil? then
            if @auto_dump then
              cursor = perform_full_dump
            else
              raise StaleCursorException, STALE_CURSOR_MSG
            end
          end
        end
      end
    end

    # Gets the current snapshot of the ns_set.
    #
    # @return [Hash<String, Set<String> >] @see #new(opt[:ns_set])
    def get_ns_set_snapshot
      # Lock usage: @ns_set
      return @ns_set.use { |ns_set, mutex| ns_set.clone }
    end

    alias_method :ns_set, :get_ns_set_snapshot

    ############################################################################
    private
    SPECIAL_COLLECTION_NAME_PATTERN = /^system\./
    MASTER_SLAVE_OPLOG_COLL_NAME = "oplog.$main"
    REPL_SET_OPLOG_COLL_NAME = "oplog.rs"
    OPLOG_NOT_FOUND_MSG = "Cannot find oplog collection. Make sure that " +
      "you are connected to a server running on master/slave or replica set configuration."
    OPLOG_AMBIGUOUS_MSG = "Cannot determine which oplog to use. Please specify " +
      "the appropriate mode."
    STALE_CURSOR_MSG = "Sync cursor is too stale: Cannot catch up with the update rate. " +
      "Please perform a manual dump."
    OPLOG_BATCH_SIZE = 200

    # Dumps the contents of a collection to Solr.
    #
    # @param namespace [String] The namespace of the collection to dump.
    # @param timestamp [BSON::Timestamp] (nil) the timestamp to use when updating the update
    #   timestamp. Nil timestamps will be ignored.
    def dump_collection(namespace, timestamp = nil)
      @logger.info "#{@name}: dumping #{namespace}..."
      db_name, coll = Util.get_db_and_coll_from_ns(namespace)

      retry_until_ok do
        @db_connection[db_name][coll].find().each do |doc|
          @solr.add(DocumentTransform.translate_doc(doc))
          # Do not update commit timestamp here since the stream of data from the
          # database is not guaranteed to be sequential in time.
        end
      end

      @solr.commit

      unless timestamp.nil?
        @config_writer.update_timestamp(namespace, timestamp)
        @config_writer.update_commit_timestamp(timestamp) 
      end

      @logger.info "#{@name}: Finished dumping #{namespace}"
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
        database, collection = Util.get_db_and_coll_from_ns(namespace)

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
      @config_writer.update_commit_timestamp(timestamp) if do_timestamp_commit
    end

    # Helper method for determining whether to apply the oplog entry changes to Solr.
    #
    # @param ns_set [Hash<String, Set<String> >] ({}) The set of namespaces to index.
    #   The key contains the name of the namespace and the value contains the names of the
    #   fields. Empty set for fields means all fields will be indexed.
    # @param namespace [String] The ns field in the oplog entry.
    #
    # @return [Boolean] true if the oplog entry should be skipped.
    def filter_entry?(ns_set, namespace)
      return !ns_set.has_key?(namespace)
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
    # @synching_mutex and @ns_set while calling this method.
    #
    # @param new_ns
    # @param checkpoint [MongoSolr::CheckpointData] The checkpoint data, ignored if nil.
    # @param is_synching [Boolean] Whether the main sync thread is running.
    # @param wait [Boolean] Waits until the newly added sets are not in a backlogged state
    #   before returning if set to true.
    # @param &block [Proc(mode, backlog)] @see #dump_and_sync
    def add_namespace(new_ns, checkpoint, is_synching, wait, &block)
      # Lock usage:
      # 1. calls dump_and_sync
      # 2. calls replay_oplog_and_sync

      if is_synching then
        new_ns.each do |ns, fields|
          if checkpoint.nil? then
            dump_and_sync(ns, wait, &block)
          else
            @oplog_backlog[ns] = []
            start_ts = checkpoint[ns]
            end_ts = retry_until_ok { get_last_oplog_timestamp }

            replay_oplog_and_sync(ns, start_ts, end_ts, false, &block)
          end
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
      # Lock usage:
      # 1. @oplog_backlog

      ns = oplog_entry["ns"]
      inserted = false

      @oplog_backlog.use do |backlog, mutex|
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
                          :selector => { "op" => { "$ne" => "n" },
                            "ts" => { "$gte" => timestamp } },
                          :order => ["$natural", :asc]
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
        @logger.error "#{@name}: #{Util.get_full_exception_msg(e)}"
        sleep @err_retry_interval
        retry
      end
    end

    # Acquires a cursor to the oplog entry with time greater than or equal to the given
    # timestamp.
    #
    # @param timestamp [BSON::Timestamp] The reference timestamp.
    #
    # @return [Mongo::Cursor] the cursor that points to the next oplog entry to the last
    #   update. nil if the last known update timestamp is too old.
    def get_oplog_cursor_w_check(timestamp)
      ret = nil

      unless timestamp.nil? then
        cursor = get_oplog_cursor(timestamp)
        doc = cursor.next_document

        if doc.nil? or not cursor.has_next? then
          # This does not necessarily mean that the cursor is too stale, since the
          # timestamp passed can be a timestamp of a no-op entry. So double check
          # if that said entry still exists.
          entry = get_oplog_collection(@mode).find_one({ "ts" => timestamp })
          ret = cursor unless entry.nil?
        elsif timestamp == doc["ts"] then
          ret = cursor
        else
          @logger.warn("#{@name}: (#{timestamp.inspect}) is too old and not in the oplog")
        end
      end

      return ret
    end

    # Creates a new thread to dump the contents of the given namespace to Solr and
    # tries to get in sync with the main thread by consuming the backlogs inserted
    # by the main thread for the given namespace while dumping.
    #
    # @param oplog_ns [String] The namespace of the collection.
    # @param wait [Boolean] Waits for the thread to finish before returning.
    # @param &block [Proc(mode, backlog)] Extra block parameter for debugging and testing.
    #   The mode can be :finished_dumping or :depleted_backlog, which describes on which
    #   stage it is running on. The backlog paramereter is the array of backlogged oplog
    #   This block will be called repeatedly during the :finished_dumping stage until the
    #   block returns (break) a false value
    def dump_and_sync(oplog_ns, wait = false, &block)
      # Lock usage:
      # 1. @oplog_backlog
      # 2. Spawns a thread that uses @oplog_backlog

      db_name, coll_name = Util.get_db_and_coll_from_ns(oplog_ns)
      db = retry_until_ok { @db_connection.db(db_name) }

      backlog_sync_thread = Thread.start do
        @oplog_backlog[oplog_ns] = []
        timestamp = retry_until_ok { get_last_oplog_timestamp }
        dump_collection(oplog_ns, timestamp)
        @solr.commit

        # For testing/debugging only
        if block_given? then
          block_val = true
          while block_val do
            block_val = yield :finished_dumping, @oplog_backlog[oplog_ns]
            # Do nothing
          end
        end
        
        consume_backlog(oplog_ns)
        yield :depleted_backlog if block_given?
      end

      backlog_sync_thread.join if wait
    end

    # Sets the state of this object to reflect the state of a given checkpoint.
    #
    # @param checkpoint_data [MongoSolr::CheckpointData] The checkpoint data.
    def restore_from_checkpoint(checkpoint_data)
      commit_ts = checkpoint_data.commit_ts

      if commit_ts.nil? then
        # Nothing to do here since we need to perform a full dump
        return
      end

      # Make sure that everything before the last commit timestamp is already indexed
      # to Solr. Possible cases of having these kind of entries include:
      #
      # 1. A newly added collection/field to index was not able to completely consume
      #    its backlog.
      checkpoint_data.each do |ns, ts|
        replay_oplog_and_sync(ns, ts, commit_ts, true)
      end
    end

    # Convenience method for initializing the cursor for sync method. Also performs a
    # database dump when this was called for the first time.
    #
    # @param checkpoint [MongoSolr::CheckpointData] The checkpoint data that can be used
    #   to resume from last session. Ignored if nil.
    #
    # @return [Mongo::Cursor] the cursor to the oplog entry, pointing to the next oplog
    #   entry to process.
    def init_sync(checkpoint)
      cursor = nil
      last_commit = nil

      unless checkpoint.nil? then
        restore_from_checkpoint(checkpoint)
        last_commit = checkpoint.commit_ts
      end

      if last_commit.nil? then
        cursor = perform_full_dump
      else
        cursor = retry_until_ok { get_oplog_cursor_w_check(last_commit) }
        cursor = perform_full_dump if cursor.nil?
      end

      return cursor
    end

    # Performs a dump of the database (filtered by the current ns_set) and returns the
    # cursor to the oplog that points to the latest entry just before the dump is
    # performed
    #
    # @return [Mongo::Cursor] the oplog cursor
    def perform_full_dump
      timestamp = nil

      cursor = retry_until_ok do
        timestamp = get_last_oplog_timestamp
        get_oplog_cursor(timestamp)
      end

      get_ns_set_snapshot.each do |namespace, field|
        dump_collection(namespace, timestamp)
      end

      return cursor
    end

    # Attempts to extract and perform operations done within a period from the oplogs.
    # Performs a dump if the given start timestamp is not in the oplog anymore.
    #
    # @param namespace [String] The name of the database.
    # @param start_ts [BSON::Timestamp] The reference timestamp to start from.
    # @param end_ts [BSON::Timestamp] The ending timestamp.
    # @param wait [Boolean] @see #dump_and_sync
    # @param &block [Proc(mode, backlog)] @see #dump_and_sync
    def replay_oplog_and_sync(namespace, start_ts, end_ts, wait, &block)
      # Lock usage:
      # 1. calls dump_and_sync
      # 2. calls update_and_sync

      oplog_coll = retry_until_ok { get_oplog_collection(@mode) }

      if start_ts.nil? then
        dump_and_sync(namespace, wait, &block)
      elsif Util.compare_bson_ts(start_ts, end_ts) == -1 then
        result = oplog_coll.find({ "ns" => namespace,
                                   "ts" => { "$gte" => start_ts, "$lte" => end_ts }},
                                 { :sort => ["$natural", :asc] })
        doc = result.next_document

        if (doc.nil? or start_ts != doc["ts"]) then
          # Timestamp is too old that the oplog entry for it is not available anymore
          dump_and_sync(namespace, wait, &block)
        else
          update_and_sync(namespace, result.to_a, wait, &block)
        end
      else
        # No new ops since last update on Solr. Advance to the end timestamp.
        @config_writer.update_timestamp(namespace, end_ts)
      end
    end

    # Creates a new thread to update the given oplog documents to Solr and tries
    # to get in sync with the main thread by consuming the backlogs inserted by
    # the main thread for the given namespace while updating Solr.
    #
    # @param namespace [String] The namespace of the oplog documents.
    # @param docs [Array<Object>] The oplog documents. Invariant: all docs should belong
    #   to the given namespace.
    # @param wait [Boolean] Waits for the thread to finish before returning.
    # @param &block [Proc(mode)] Extra block parameter for debugging and testing.
    #   The mode can be :finished_updating or :depleted_backlog, which describes on which
    #   stage it is running on. The backlog paramereter is the array of backlogged oplog
    #   This block will be called repeatedly during the :finished_dumping stage until the
    #   block returns (break) a false value.
    def update_and_sync(namespace, docs, wait = false, &block)
      # Lock usage:
      # 1. @oplog_backlog
      # 2. Spawns a thread that uses @oplog_backlog

      sync_thread = Thread.start do
        @oplog_backlog[namespace] = []
        update_solr(docs)

        # For testing/debugging only
        if block_given? then
          block_val = true

          while block_val do
            block_val = yield :finished_updating, @oplog_backlog[namespace]
            # Do nothing
          end
        end

        consume_backlog(namespace)
        yield :depleted_backlog if block_given?        
      end

      sync_thread.join if wait
    end

    # Consumes all oplogs that have accumulated by the main thread (sync) while
    # performing a background task.
    #
    # @param namespace [String] The namespace of the collection to consume.
    def consume_backlog(namespace)
      # Lock usage:
      # 1. @oplog_backlog

      @oplog_backlog.use do |backlog, mutex|
        until backlog[namespace].empty?
          oplog_entries = backlog[namespace]
          backlog[namespace] = []

          mutex.unlock
          update_solr(oplog_entries)
          mutex.lock
        end

        backlog.delete(namespace)
      end
    end
  end
end

