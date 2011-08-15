require_relative "solr_config_const"
require_relative "util"

module MongoSolr
  # A simple class for writing updates to the configuration database.
  class ConfigWriter
    include Util

    # @param coll [Mongo::Collection] The configuration database collection.
    # @param solr_loc [String] The location of the Solr Server
    def initialize(coll, solr_loc)
      @solr_loc = solr_loc
      @coll = coll
    end

    # Updates the timestamp for the given namespace. Updates will only happen if the
    # entry for the namespace exists. The reason for this is that the given namespace
    # may already have been removed by the client for indexing.
    #
    # @param namespace [String] The whole namespace name of the collection to update.
    # @param new_timestamp [BSON::Timestamp] The new timestamp
    def update_timestamp(namespace, new_timestamp)
      @coll.update({ SolrConfigConst::SOLR_URL_KEY => @solr_loc,
                     SolrConfigConst::NS_KEY => namespace
                   },
                   { "$set" => {
                       SolrConfigConst::UPDATE_TIMESTAMP_KEY => new_timestamp
                     }})
    end

    # Updates the timestamp when a commit is performed.
    #
    # @param timestamp {BSON::Timestamp} The new timestamp
    def update_commit_timestamp(timestamp)
      @coll.update({ SolrConfigConst::SOLR_URL_KEY => @solr_loc },
                   { "$set" => { SolrConfigConst::COMMIT_TIMESTAMP_KEY => timestamp }})
    end

    # Updates the total documents to be dumped for the given collection.
    #
    # @param namespace [String] The namespace of the collection.
    # @param count [Integer] The new count.
    def update_total_dump_count(namespace, count)
      @coll.update({ SolrConfigConst::SOLR_URL_KEY => @solr_loc,
                     SolrConfigConst::NS_KEY => namespace
                   },
                   { "$set" => { SolrConfigConst::TOTAL_TO_DUMP_KEY => count }})
    end

    # Resets the counter for the docs dumped.
    #
    # @param namespace [String] The namespace of the collection.
    def reset_dump_count(namespace)
      @coll.update({ SolrConfigConst::SOLR_URL_KEY => @solr_loc,
                     SolrConfigConst::NS_KEY => namespace
                   },
                   { "$set" => { SolrConfigConst::DOCS_DUMPED_KEY => 0 }})
    end

    # Increments the counter for the docs dumped.
    #
    # @param namespace [String] The namespace of the collection.
    def increment_dump_count(namespace)
      @coll.update({ SolrConfigConst::SOLR_URL_KEY => @solr_loc,
                     SolrConfigConst::NS_KEY => namespace
                   },
                   { "$inc" => { SolrConfigConst::DOCS_DUMPED_KEY => 1 }})
    end
  end
end

