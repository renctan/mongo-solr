require_relative "test_helper"
require "#{PROJ_SRC_PATH}/solr_config_const"
require "bson"

# Simple fixture class that contains several mock configuration documents.
class ConfigDBFixture
  include MongoSolr

  SOLR_LOC_1 = "http://localhost:8983/solr"
  CONFIG1 =
  [
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_1,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(100, 1),
     SolrConfigConst::NS_KEY => "courses.undergrad",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => BSON::Timestamp.new(10, 1),
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_1,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(100, 1),
     SolrConfigConst::NS_KEY => "courses.masters",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => BSON::Timestamp.new(20, 1),
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_1,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(100, 1),
     SolrConfigConst::NS_KEY => "courses.doctoral",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => BSON::Timestamp.new(20, 3),
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_1,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(100, 1),
     SolrConfigConst::NS_KEY => "staff.prof",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => BSON::Timestamp.new(20, 4),
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_1,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(100, 1),
     SolrConfigConst::NS_KEY => "staff.admin",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => BSON::Timestamp.new(40, 1),
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   }
  ]

  SOLR_LOC_2 = "http://somewhere.out.there:4321/solr"
  CONFIG2 =
  [
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_2,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(200, 1),
     SolrConfigConst::NS_KEY => "courses.undergrad",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => BSON::Timestamp.new(111, 1),
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_2,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(200, 1),
     SolrConfigConst::NS_KEY => "courses.masters",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => BSON::Timestamp.new(111, 14),
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   }
  ]

  SOLR_LOC_3 = "http://royal.chocolate.flush:123/solr"
  CONFIG3 =
  [
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_3,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(321, 1),
     SolrConfigConst::NS_KEY => "courses.doctoral",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => BSON::Timestamp.new(111, 1),
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_3,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(321, 1),
     SolrConfigConst::NS_KEY => "staff.prof",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => BSON::Timestamp.new(444, 1),
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_3,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(321, 1),
     SolrConfigConst::NS_KEY => "staff.admin",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => BSON::Timestamp.new(321, 200),
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   }
  ]
end

