require "rubygems"
require "rsolr"
require "bson"

require_relative "solr_synchronizer"
require_relative "config_format_reader"
require_relative "util"

module MongoSolr
  # A simple class for cleaning the documents marked for deletion by the SolrSynchronizer.
  class Cleanup
    include Util

    # Runs the cleanup routine. This is a blocking call that runs an infinite loop.
    #
    # @param config_source [MongoSolr::ConfigSource] The object that contains the
    #   configuration information for all the different Solr Servers.
    # @param cleanup_interval [Number] The time in seconds between each cleanup operation.
    # @param old_time [Number] The time in seconds where a document for deletion is
    #   considered old enough and will be deleted.
    def self.run(config_source, cleanup_interval, old_time)
      loop do
        config_source.each do |config_data|
          solr_config = ConfigFormatReader.new(config_data)
          solr = RSolr.connect(:url => solr_config.solr_loc)

          for_deletion_time = Time.now.tv_sec - old_time
          ts = bsonts_to_long(BSON::Timestamp.new(for_deletion_time, 1))

          query = "#{SolrSynchronizer::SOLR_DELETED_FIELD}:true"
          query += " AND #{SolrSynchronizer::SOLR_TS_FIELD}:[* TO #{ts}]"
          solr.delete_by_query(query)
          solr.commit
        end

        sleep cleanup_interval
      end
    end
  end
end

