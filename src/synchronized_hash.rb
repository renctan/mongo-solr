module MongoSolr
  # A simple wrapper class for accessing a hash with a mutex.
  class SynchronizedHash
    def initialize
      @hash_mutex = Mutex.new

      # Protected by @hash_mutex
      @hash = {}
    end

    # Use the hash.  Use this method if you want to hold the lock while doing several
    # operations.
    #
    # @param block [Proc(hash [Hash], lock [Mutex])] The code to execute when using the hash.
    #   The entire block is executed while holding a lock. The lock object is also provided
    #   to more fine grain control like releasing the lock.
    #
    # @return [Object] the return value of the block
    def use(&block)
      @hash_mutex.synchronize { yield @hash, @hash_mutex }
    end

    def method_missing(sym, *args, &block)
      @hash_mutex.synchronize { @hash.send(sym, *args, &block) }
    end
  end
end

