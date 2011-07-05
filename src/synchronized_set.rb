require "set"

module MongoSolr
  # A simple wrapper class for accessing a set with a mutex.
  class SynchronizedSet
    # @param hash [Set] (Emtpy set) An initial set to populate this object.
    def initialize(set = Set.new)
      @set_mutex = Mutex.new

      # Protected by @set_mutex
      @set = set.clone
    end

    # Use the set. Use this method if you want to hold the lock while doing several operations.
    #
    # @param block [Proc(set [Set], lock [Mutex])] The code to execute when using the set.
    #   The entire block is executed while holding a lock. The lock object is also provided
    #   to more fine grain control like releasing the lock.
    #
    # @return [Object] the return value of the block
    def use(&block)
      @set_mutex.synchronize { yield @set, @set_mutex }
    end

    def method_missing(sym, *args, &block)
      @set_mutex.synchronize { @set.send(sym, *args, &block) }
    end
  end
end

