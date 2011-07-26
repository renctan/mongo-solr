require File.expand_path("../../test_helper", __FILE__)
require "#{PROJ_SRC_PATH}/solr_retry_decorator"

class SolrRetryDecoratorTest < Test::Unit::TestCase
  context "Basic Test" do
    setup do
      @error = states("error").starts_as("yes")
      @solr = mock()
    end

    should "retry add with correct parameters" do
      doc = { :_id => 1, :x => 3 }

      @solr.stubs(:add).raises(RuntimeError).when(@error.is("yes")).then(@error.is("no"))
      @solr.expects(:add).once.with(doc).when(@error.is("no"))

      solr_retry = MongoSolr::SolrRetryDecorator.new(@solr, 0, nil)
      solr_retry.add doc
    end

    should "retry commit with correct parameters" do
      @solr.stubs(:commit).raises(RuntimeError).when(@error.is("yes")).then(@error.is("no"))
      @solr.expects(:commit).once.with().when(@error.is("no"))

      solr_retry = MongoSolr::SolrRetryDecorator.new(@solr, 0, nil)
      solr_retry.commit
    end

    should "retry delete_by_id with correct parameters" do
      id = 1234567890

      @solr.stubs(:delete_by_id).
        raises(RuntimeError).when(@error.is("yes")).then(@error.is("no"))
      @solr.expects(:delete_by_id).once.with(id).when(@error.is("no"))

      solr_retry = MongoSolr::SolrRetryDecorator.new(@solr, 0, nil)
      solr_retry.delete_by_id id
    end

    should "log when exception occured" do
      @solr.stubs(:add).raises(RuntimeError).then.returns(nil)

      logger = mock()
      logger.expects(:error).once

      solr_retry = MongoSolr::SolrRetryDecorator.new(@solr, 0, logger)
      solr_retry.add
    end
  end
end

