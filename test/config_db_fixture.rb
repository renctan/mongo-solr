require_relative "test_helper"
require "#{PROJ_SRC_PATH}/solr_config_const"
require "bson"

# Simple fixture class that contains several mock configuration documents.
class ConfigDBFixture
  include MongoSolr

  CONFIG1 =
  {
    SolrConfigConst::SOLR_URL_KEY => "http://localhost::8983/solr",
    SolrConfigConst::TIMESTAMP_KEY => BSON::Timestamp.new(100, 1),
    SolrConfigConst::LIST_KEY =>
    [
     {
       SolrConfigConst::NS_KEY => "courses.undergrad",
       SolrConfigConst::TIMESTAMP_KEY => BSON::Timestamp.new(10, 1),
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "courses.masters",
       SolrConfigConst::TIMESTAMP_KEY => BSON::Timestamp.new(20, 1),
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "courses.doctoral",
       SolrConfigConst::TIMESTAMP_KEY => BSON::Timestamp.new(20, 3),
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "staff.prof",
       SolrConfigConst::TIMESTAMP_KEY => BSON::Timestamp.new(20, 4),
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "staff.admin",
       SolrConfigConst::TIMESTAMP_KEY => BSON::Timestamp.new(40, 1),
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
    ]
  }

  CONFIG2 =
  {
    SolrConfigConst::SOLR_URL_KEY => "http://localhost:8983/solr",
    SolrConfigConst::TIMESTAMP_KEY => BSON::Timestamp.new(200, 1),
    SolrConfigConst::LIST_KEY =>
    [
     {
       SolrConfigConst::NS_KEY => "courses.undergrad",
       SolrConfigConst::TIMESTAMP_KEY => BSON::Timestamp.new(111, 1),
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "courses.masters",
       SolrConfigConst::TIMESTAMP_KEY => BSON::Timestamp.new(111, 14),
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     }
    ]
  }

  CONFIG3 =
  {
    SolrConfigConst::SOLR_URL_KEY => "http://somewhere.out.there:4321/solr",
    SolrConfigConst::TIMESTAMP_KEY => BSON::Timestamp.new(321, 1),
    SolrConfigConst::LIST_KEY =>
    [
     {
       SolrConfigConst::NS_KEY => "courses.doctoral",
       SolrConfigConst::TIMESTAMP_KEY => BSON::Timestamp.new(111, 1),
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "staff.prof",
       SolrConfigConst::TIMESTAMP_KEY => BSON::Timestamp.new(444, 1),
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "staff.admin",
       SolrConfigConst::TIMESTAMP_KEY => BSON::Timestamp.new(321, 200),
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     }
    ]
  }
end

