module MongoSolr
  # Raised when the oplog collection cannot be found in the database
  class OplogNotFoundException < RuntimeError
  end
end
