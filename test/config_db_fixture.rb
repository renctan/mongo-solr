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

  # Config with no timestamp
  SOLR_LOC_4 = "http://royal.chocolate.flush:123/solr"
  CONFIG4 =
  [
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_4,
     SolrConfigConst::NS_KEY => "courses.doctoral",
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   }
  ]

  SHARD_1 = "shard0000"
  SHARD_2 = "myset"

  SHARD_CONFIG1 =
  [
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_1,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(100, 1),
     SolrConfigConst::NS_KEY => "courses.undergrad",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => {
       SHARD_1 => BSON::Timestamp.new(10, 1),
       SHARD_2 => BSON::Timestamp.new(20, 1)
     },
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_1,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(100, 1),
     SolrConfigConst::NS_KEY => "courses.masters",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => {
       SHARD_1 => BSON::Timestamp.new(30, 1),
       SHARD_2 => BSON::Timestamp.new(40, 1)
     },
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_1,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(100, 1),
     SolrConfigConst::NS_KEY => "courses.doctoral",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => {
       SHARD_1 => BSON::Timestamp.new(30, 5),
       SHARD_2 => BSON::Timestamp.new(40, 123)
     },
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_1,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(100, 1),
     SolrConfigConst::NS_KEY => "staff.prof",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => {
       SHARD_1 => BSON::Timestamp.new(10, 16),
       SHARD_2 => BSON::Timestamp.new(26, 100)
     },
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_1,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(100, 1),
     SolrConfigConst::NS_KEY => "staff.admin",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => {
       SHARD_1 => BSON::Timestamp.new(66, 1),
       SHARD_2 => BSON::Timestamp.new(44, 1)
     },
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   }
  ]

  SHARD_CONFIG2 =
  [
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_2,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(200, 1),
     SolrConfigConst::NS_KEY => "courses.undergrad",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => {
       SHARD_1 => BSON::Timestamp.new(111, 1),
       SHARD_2 => BSON::Timestamp.new(133, 1)
     },
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_2,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(200, 1),
     SolrConfigConst::NS_KEY => "courses.masters",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => {
       SHARD_1 => BSON::Timestamp.new(11, 1111),
       SHARD_2 => BSON::Timestamp.new(199, 2222)
     },
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   }
  ]

  SHARD_CONFIG3 =
  [
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_3,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(321, 1),
     SolrConfigConst::NS_KEY => "courses.doctoral",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => {
       SHARD_1 => BSON::Timestamp.new(123, 1),
       SHARD_2 => BSON::Timestamp.new(234, 1)
     },
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_3,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(321, 1),
     SolrConfigConst::NS_KEY => "staff.prof",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => {
       SHARD_1 => BSON::Timestamp.new(444, 1),
       SHARD_2 => BSON::Timestamp.new(234, 1)
     },
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   },
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_3,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(321, 1),
     SolrConfigConst::NS_KEY => "staff.admin",
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => {
       SHARD_1 => BSON::Timestamp.new(321, 123),
       SHARD_2 => BSON::Timestamp.new(321, 9999)
     },
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
   }
  ]

  # Config with partial timestamp
  SHARD_CONFIG4 =
  [
   {
     SolrConfigConst::SOLR_URL_KEY => SOLR_LOC_4,
     SolrConfigConst::COMMIT_TIMESTAMP_KEY => BSON::Timestamp.new(87654, 1),
     SolrConfigConst::NS_KEY => "courses.doctoral",
     SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 },
     SolrConfigConst::UPDATE_TIMESTAMP_KEY => {
       SHARD_2 => BSON::Timestamp.new(4263897, 3452)
     }
   }
  ]
end

