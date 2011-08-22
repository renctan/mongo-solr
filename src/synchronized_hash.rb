require "forwardable"
require_relative "mutex_obj_pair"

module MongoSolr
  # A simple wrapper class for accessing a hash with a mutex.
  class SynchronizedHash
    extend Forwardable

    def_delegator :@hash, :use

    # @param hash [Hash] ({}) An initial hash to populate this object.
    def initialize(hash = {})
      @hash = MutexObjPair.new(hash.clone)
    end

    def method_missing(sym, *args, &block)
      @hash.use { |hash, mutex| hash.send(sym, *args, &block) }
    end
  end
end

