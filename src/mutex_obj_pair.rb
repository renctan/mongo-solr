module MongoSolr
  # A simple class that pairs a mutex to an object.
  class MutexObjPair
    # @param obj [Object] The object to pair with a mutex.
    def initialize(obj)
      @obj = obj
      @mutex = Mutex.new
    end

    # Use the stored object while holding the lock.
    #
    # @param block [Proc(obj [Object], lock [Mutex])] The code to execute when using the stored
    #   object. The entire block is executed while holding a lock. The lock object is also
    #   provided for more fine grain control like releasing the lock within the block.
    #
    # @return [Object] the return value of the block
    def use(&block)
      @mutex.synchronize { yield @obj, @mutex }
    end

    # Sets a new object to be stored.
    #
    # @param obj [Object] The new object.
    def set(obj)
      @mutex.synchronize { @obj = obj }
    end

    # Sets a new object to be stored with out locking.
    #
    # @param obj [Object] The new object.
    def set_wo_lock(obj)
      @obj = obj
    end
  end
end

