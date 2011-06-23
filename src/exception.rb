module MongoSolr
  # Raised on the following cases:
  # 1. The oplog collection cannot be found in the database
  # 2. Cannot determine which oplog collection to use (when mode is not given).
  # 3. Authentication failure on the local db which contains the oplog collection.
  class OplogException < RuntimeError
  end
end
