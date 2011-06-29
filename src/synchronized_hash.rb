module MongoSolr
  # A simple wrapper class for accessing a hash with a mutex.
  class SynchronizedHash
    def initialize
      @hash_mutex = Mutex.new

      # Protected by @hash_mutex
      @hash = {}
    end

    # Use the hash.
    #
    # @param block [Proc(hash [Hash], lock [Mutex])] The code to execute when using the hash.
    #   The entire block is executed while holding a lock. The lock object is also provided
    #   to more fine grain control like releasing the lock.
    #
    # @return [Object] the return value of the block
    def use(&block)
      @hash_mutex.synchronize { yield @hash, @hash_mutex }
    end
  end
end

