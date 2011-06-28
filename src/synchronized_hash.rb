require "hamster/hash"

module MongoSolr
  # A simple thread-safe hash collection that wraps around the Hamster hash class which assigns the
  # copies made during some operations to itself.
  #
  # @see Hamster.Hash
  class SynchronizedHash
    def initialize
      @hash = Hamster.hash
    end

    # Assign a new value for the key
    def []=(key, value)
      @hash = @hash.put(key, value)
    end

    # Delete a key
    def delete(key)
      @hash = @hash.delete(key)
    end

    def method_missing(sym, *args, &block)
      @hash.send(sym, *args, &block)
    end
  end
end

