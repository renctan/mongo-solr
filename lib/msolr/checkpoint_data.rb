require "forwardable"
require "hamster"

module MongoSolr
  # A simple class for holding relevant checkpoint information to be used by SolrSynchronizer.
  class CheckpointData
    extend Forwardable

    attr_reader :commit_ts
    def_delegators :@namespace_set, :each, :[], :empty?

    # @param commit_timestamp [BSON::Timestamp] The commit timestamp 
    # @param namespace_set [Hash<String, BSON::Timestamp>] The structure that contains
    #   the update timestamp for each namespace.
    def initialize(commit_timestamp, namespace_set)
      @commit_ts = commit_timestamp
      @namespace_set = Hamster.hash(namespace_set)
    end
  end
end

