require "rubygems"
require "mongo"
require "set"
require "logger"
require "hamster"

require_relative "document_transform"
require_relative "exception"
require_relative "synchronized_hash"
require_relative "solr_retry_decorator"
require_relative "util"

module MongoSolr
  # A simple class utility for indexing an entire MongoDB database contents (excluding
  # administrative collections) to Solr.
  class SolrSynchronizer
    include Util

    SOLR_TS_FIELD = "$ts"
    SOLR_NS_FIELD = "$ns"
    SOLR_DELETED_FIELD = "$deleted"

    # Creates a synchronizer instance.
    #
    # @param solr [RSolr::Client] The client to the Solr server to populate database documents.
    # @param mongo_connection [Mongo::Connection] The connection to the database to synchronize.
    # @param oplog_coll [Mongo::Collection] The oplog collection to monitor.
    # @param config_writer [MongoSolr::ConfigWriter] The object that can be used to update
    #   the configuration.
    #
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
    # @opt [Boolean] :is_sharded (false) Set to true if connected to a sharded server.
    #
    # Note: This object assumes that the solr and mongo_connection params are valid. As a
    #   a consequence, it will keep on retrying whenever an exception on these objects
    #   occured.
    #
    # Example:
    #  mongo = Mongo::Connection.new("localhost", 27017)
    #  solr_client = RSolr.connect(:url => "http://localhost:8983/solr")
    #  solr = MongoSolr::SolrSynchronizer.new(solr_client, mongo, :master_slave)
    def initialize(solr, mongo_connection, oplog_coll, config_writer, opt = {})
      @db_connection = mongo_connection
      @logger = opt[:logger] || Logger.new(STDOUT)
      @name = opt[:name] || ""
      @update_interval = opt[:interval] || 0
      @err_retry_interval = opt[:err_retry_interval] || 1
      @auto_dump = opt[:auto_dump] || false
      @is_sharded = opt[:is_sharded] || false

      @oplog_coll = oplog_coll
      @config_writer = config_writer

      @solr = SolrRetryDecorator.new(solr, @err_retry_interval, @logger)

      # The set of collections to listen to for updates in the oplogs.
      ns_set = opt[:ns_set] || {}
      @ns_set = Hamster.hash(ns_set)
      @checkpoint = opt[:checkpt]

      # The oplog data for collections that are still in the process of dumping.
      @oplog_backlog = MongoSolr::SynchronizedHash.new

      # Locks should be taken in this order to avoid deadlock
      @stop_mutex = Mutex.new
      @synching_mutex = Mutex.new

      # implicit ns_set mutex
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
    # @option opt [Hash<String, Set<String> >] :ns_set ({}) @see #new
    # @opt [CheckpointData] :checkpt (nil) This will be used to continue from a previous
    #   session if given.
    # @opt [Boolean] :wait (false) Waits until the newly added sets are not in a backlogged state
    #   before returning if set to true.
    # @param block [Proc] @see #dump_and_sync
    def update_config(opt = {}, &block)
      # Lock usage:
      # 1. @synching_mutex->add_namespace()

      wait = opt[:wait] || false
      checkpoint_opt = opt[:checkpt]

      @checkpoint = checkpoint_opt unless checkpoint_opt.nil?
      new_ns_set = opt[:ns_set]

      unless new_ns_set.nil? then
        @synching_mutex.synchronize do
          diff = new_ns_set.reject { |k, v| @ns_set.has_key?(k) }
          add_namespace(diff, checkpoint_opt, @is_synching, wait, &block)

          @ns_set = Hamster.hash(new_ns_set)
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
    #   4. block(:cursor_reset, doc_count) will be called everytime an exception occured and the
    #      cursor was updated.
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
      # 2. insert_to_backlog()
      # 3. stop_synching?()

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

      last_timestamp = (@checkpoint.nil? ? nil : @checkpoint.commit_ts)
      cursor = init_sync(@checkpoint)

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
          rescue => e
            @logger.error "#{@name}: #{get_full_exception_msg(e)}"
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

          yield :cursor_reset, doc_count if block_given?
        end
      end
    end

    # Gets the current snapshot of the ns_set.
    #
    # @return [Hash<String, Set<String> >] @see #new(opt[:ns_set])
    def get_ns_set_snapshot
      return @ns_set
    end

    alias_method :ns_set, :get_ns_set_snapshot

    ############################################################################
    private
    SPECIAL_COLLECTION_NAME_PATTERN = /^system\./
    STALE_CURSOR_MSG = "Sync cursor is too stale: Cannot catch up with the update rate. " +
      "Please perform a manual dump."
    OPLOG_BATCH_SIZE = 200

    # Dumps the contents of a collection to Solr.
    #
    # @param namespace [String] The namespace of the collection to dump.
    # @param timestamp [BSON::Timestamp] the timestamp to use when updating the update
    #   timestamp.
    def dump_collection(namespace, timestamp)
      @logger.info "#{@name}: dumping #{namespace}..."
      db_name, coll = get_db_and_coll_from_ns(namespace)

      retry_until_ok do
        if @is_sharded then
          # Use the connection from oplog (which is to the shard itself) since 
          # @db_connection is to the mongos, which will return results from all shards!
          cursor = @oplog_coll.db.connection[db_name][coll].find()
        else
          cursor = @db_connection[db_name][coll].find()
        end

        @config_writer.update_total_dump_count(namespace, cursor.count)
        @config_writer.reset_dump_count(namespace)

        cursor.each do |doc|
          @solr.add(prepare_solr_doc(doc, namespace, timestamp))
          # Do not update commit timestamp here since the stream of data from the
          # database is not guaranteed to be sequential in time.

          @config_writer.increment_dump_count(namespace)
        end
      end

      @solr.commit

      retry_until_ok do
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
          if @is_sharded then
            id = oplog_entry["o"]["_id"]
            db_name, coll_name = get_db_and_coll_from_ns(namespace)

            # Check if document exists in the mongos. If it does, it implies that this delete
            # operation is part of a chunk migration.
            count = @db_connection[db_name][coll_name].find({ "_id" => id }).count
            next if count >= 1
          end

          @logger.info "#{@name}: adding #{doc.inspect}"
          @solr.add(prepare_solr_doc(doc, namespace, timestamp))
          retry_until_ok { @config_writer.update_timestamp(namespace, timestamp) }

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

          if @is_sharded then
            db_name, coll_name = get_db_and_coll_from_ns(namespace)

            # Check if document exists in the mongos. If it does, it implies that this delete
            # operation is part of a chunk migration.
            count = @db_connection[db_name][coll_name].find({ "_id" => id }).count
            next if count >= 1
          end

          to_update.delete(id)

          @logger.info "#{@name}: marked as deleted: #{doc.inspect}"
          new_doc = prepare_solr_doc({ "_id" => id }, namespace, timestamp)
          new_doc[SOLR_DELETED_FIELD] = true
          @solr.add(new_doc)

          retry_until_ok { @config_writer.update_timestamp(namespace, timestamp) }

        when "n" then
          # NOOP: do nothing

        else
          @logger.error "#{@name}: Unrecognized operation in oplog entry: #{oplog_entry.inspect}"
        end
      end

      update_list.each do |namespace, id_list|
        database, collection = get_db_and_coll_from_ns(namespace)

        retry_until_ok do
          to_update = @db_connection.db(database).collection(collection).
            find({"_id" => {"$in" => id_list.to_a}})

          to_update.each do |doc|
            @logger.info "#{@name}: updating #{doc.inspect}"
            @solr.add(prepare_solr_doc(doc, namespace, timestamp))
            # Use the last timestamp from oplog_doc_entries.
            retry_until_ok { @config_writer.update_timestamp(namespace, timestamp) }

            # Remove from the set so there will be less documents to update
            # when an exception occurs and this block is executed again
            id_list.delete doc["_id"]
          end
        end
      end

      @solr.commit
      if do_timestamp_commit then
        retry_until_ok { @config_writer.update_commit_timestamp(timestamp) }
      end
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

    # Adds a collection without using a lock. Caller should be holding the lock for
    # @synching_mutex while calling this method.
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
        @oplog_coll.find({}, :sort => ["$natural", :desc], :limit => 1)
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
      Mongo::Cursor.new(@oplog_coll,
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
      rescue OplogException, StaleCursorException
        raise
      rescue => e
        @logger.error "#{@name}: #{get_full_exception_msg(e)}"
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
          entry = @oplog_coll.find_one({ "ts" => timestamp })

          if entry.nil? then
            less_than_doc = @oplog_coll.find_one({ "ts" => { "$lt" => timestamp }})

            unless less_than_doc.nil? then
              ret = get_oplog_cursor_w_check(rollback)
            end
          else
            ret = cursor
          end
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

      db_name, coll_name = get_db_and_coll_from_ns(oplog_ns)
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
        unless ts == commit_ts then
          replay_oplog_and_sync(ns, ts, commit_ts, true)
        end
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

      if start_ts.nil? then
        dump_and_sync(namespace, wait, &block)
      elsif compare_bson_ts(start_ts, end_ts) == -1 then
        result = nil

        doc = retry_until_ok do
          result = @oplog_coll.find({ "ns" => namespace,
                             "ts" => { "$gte" => start_ts, "$lte" => end_ts }},
                           { :sort => ["$natural", :asc] })
          result.next_document
        end

        if (doc.nil? or start_ts != doc["ts"]) then
          # Timestamp is too old that the oplog entry for it is not available anymore
          dump_and_sync(namespace, wait, &block)
        else
          update_and_sync(namespace, result.to_a, wait, &block)
        end
      else
        # No new ops since last update on Solr. Advance to the end timestamp.
        retry_until_ok { @config_writer.update_timestamp(namespace, end_ts) }
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

    # Prepares the document for indexing to Solr.
    #
    # @param doc [Object] The Mongo document to index to Solr
    # @param ns [String] The namespace of the document
    # @param ts [BSON::Timestamp] The timestamp to embed to this document
    #
    # @return [Object] the pre-formatted document to send to Solr.
    def prepare_solr_doc(doc, ns, ts)
      ret_doc = DocumentTransform.translate_doc(filter_doc(doc, ns)).merge({
        SOLR_TS_FIELD => bsonts_to_long(ts),
        SOLR_NS_FIELD => ns
      })

      return ret_doc
    end

    # Removes fields that are not specified in the index configuration document.
    #
    # @param doc [Object] The Mongo document to filter.
    # @param ns [String] The namespace of the document
    #
    # @return [Object] the filtered document
    def filter_doc(doc, ns)
      fields = @ns_set[ns]

      unless fields.nil? or fields.empty? then
        new_doc = doc.reject { |k, v| !(fields.include?(k)) }
        new_doc["_id"] = doc["_id"]
        doc = new_doc
      end

      return doc
    end

    # Performs a rollback on the Solr server.
    # 
    # @return [BSON::Timestamp] the timestamp of the oplog entry less than the timestamp
    #   of the oldest document being rollback to.
    def rollback
      response = @solr.get("select",
                           { :params => { :q => "#{SOLR_TS_FIELD}:*",
                               :sort => "#{SOLR_TS_FIELD} desc", :rows => 1 }})

      latest_solr_doc = response["response"]["docs"].first

      if latest_solr_doc.nil? then
        return nil
      else
        solr_ts = latest_solr_doc[SOLR_TS_FIELD]
        doc = retry_until_ok do
          @oplog_coll.find_one({ "ts" => { "$lt" => long_to_bsonts(solr_ts) }},
                               :sort => ["$natural", :desc])
        end

        return nil if doc.nil?

        # Basically, the rollback window is the time between the latest timestamp in Solr
        # and the largest timestamp value in the oplog less than the latest timestamp in
        # Solr. The assumption here is that there can be only one primary at any time
        # (which is true as of MongoDB v1.8.3) and as a consequence, there is only one
        # unique timeline for the entire history of a replica set.
        rollback_cutoff_timestamp = doc["ts"]
        start_ts = bsonts_to_long(rollback_cutoff_timestamp)
        end_ts = solr_ts

        query = "#{SOLR_TS_FIELD}:[#{start_ts} TO #{end_ts}]"
        response = @solr.get("select", { :params => { :q => query }})
        docs_to_rollback = response["response"]["docs"]

        rollback_set = {}
        docs_to_rollback.each do |solr_doc|
          ns = solr_doc[SOLR_NS_FIELD]

          # No need to use Set since each entry in the Solr server should have a unique _id
          if rollback_set.has_key? ns then
            rollback_set[ns] << solr_doc["_id"]
          else
            rollback_set[ns] = [solr_doc["_id"]]
          end
        end

        rollback_set.each do |namespace, id_list|
          database, collection = get_db_and_coll_from_ns(namespace)
          bson_obj_id_list = id_list.map { |x| BSON::ObjectId(x) }

          to_update = retry_until_ok do
            # Assumption for using to_a: the size of the id_list is small
            @db_connection[database][collection].
              find({ "_id" => { "$in" => bson_obj_id_list }}).to_a
          end

          id_list_set = Set.new(id_list)
          to_index = []
          to_update.each do |doc|
            id_list_set.delete(doc["_id"].to_s) # Note that doc._id is in BSON::ObjectId
            to_index << prepare_solr_doc(doc, namespace, rollback_cutoff_timestamp)
          end

          # This set now contains the new inserts that where the rolled back. That is why
          # why the new primary does not have a document for it.
          id_list_set.each do |id|
            new_doc = prepare_solr_doc({ "_id" => id }, namespace, rollback_cutoff_timestamp)
            new_doc[SOLR_DELETED_FIELD] = true
            to_index << new_doc
          end

          @solr.add(to_index)
          @logger.info("#{@name}: Rolling back the following docs:\n#{to_index.inspect}")
        end

        @solr.commit
        retry_until_ok { @config_writer.update_commit_timestamp(rollback_cutoff_timestamp) }

        return rollback_cutoff_timestamp
      end
    end
  end
end

