require "set"

module MongoSolr
  # A simple wrapper class for accessing a set with a mutex.
  class SynchronizedSet
    def initialize
      @set_mutex = Mutex.new

      # Protected by @set_mutex
      @set = Set.new
    end

    # Use the set.
    #
    # @param block [Proc(set [Set], lock [Mutex])] The code to execute when using the set.
    #   The entire block is executed while holding a lock. The lock object is also provided
    #   to more fine grain control like releasing the lock.
    #
    # @return [Object] the return value of the block
    def use(&block)
      @set_mutex.synchronize { yield @set, @set_mutex }
    end
  end
end

