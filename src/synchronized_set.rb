require "hamster/set"

module MongoSolr
  # A simple thread-safe set collection that wraps around the Hamster set class which assigns the
  # copies made during some operations to itself.
  #
  # @see Hamster.Set
  class SynchronizedSet
    def initialize
      @set = Hamster.set
    end

    # Add an item to the set
    def add(item)
      @set = @set.add(item)
    end

    # Delete an item from the set
    def delete(item)
      @set = @set.delete(item)
    end

    def method_missing(sym, *args, &block)
      @set.send(sym, *args, &block)
    end
  end 
end

