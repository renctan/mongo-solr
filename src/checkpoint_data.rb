require "forwardable"

module MongoSolr
  # A simple class for holding relevant checkpoint information to be used by SolrSynchronizer.
  class CheckpointData
    extend Forwardable

    attr_reader :commit_ts
    def_delegator :@namespace_list, :each
    def_delegator :@namespace_list, :[]=, :set

    # @param commit_timestamp [BSON::Timestamp] The commit timestamp 
    def initialize(commit_timestamp)
      @commit_ts = commit_timestamp
      @namespace_list = {}
    end
  end
end

