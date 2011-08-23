module MongoSolr
  # Simple class for piggy backing the contents of an existing exception.
  class ExceptionWrapper < RuntimeError
    attr_reader :excep

    # @param orig_excep [Exception] An exception object to store.
    def initialize(orig_excep)
      @excep = orig_excep
    end
  end

  # Raised on the following cases:
  # 1. The oplog collection cannot be found in the database
  # 2. Cannot determine which oplog collection to use (when mode is not given).
  # 3. Authentication failure on the local db which contains the oplog collection.
  class OplogException < RuntimeError
  end

  # Raised on the following cases:
  # 1. When the oplog cursor becomes too stale to the point that the oplog has been
  #    rolled over.
  class StaleCursorException < RuntimeError
  end

  # Raised on the following cases:
  # 1. When an exception is not resolved even when a SolrSynchronizer object needs to stop.
  class RetryFailedException < ExceptionWrapper
  end
end

