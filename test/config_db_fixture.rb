require_relative "test_helper"
require "#{PROJ_SRC_PATH}/solr_config_const"

# Simple fixture class that contains several mock configuration documents.
class ConfigDBFixture
  include MongoSolr

  CONFIG1 =
  {
    SolrConfigConst::SOLR_URL_KEY => "http://localhost::8983/solr",
    SolrConfigConst::LIST_KEY =>
    [
     {
       SolrConfigConst::NS_KEY => "courses.undergrad",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "courses.masters",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "courses.doctoral",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "staff.prof",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "staff.admin",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
    ]
  }

  CONFIG2 =
  {
    SolrConfigConst::SOLR_URL_KEY => "http://localhost:8983/solr",
    SolrConfigConst::LIST_KEY =>
    [
     {
       SolrConfigConst::NS_KEY => "courses.undergrad",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "courses.masters",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     }
    ]
  }

  CONFIG3 =
  {
    SolrConfigConst::SOLR_URL_KEY => "http://somewhere.out.there:4321/solr",
    SolrConfigConst::LIST_KEY =>
    [
     {
       SolrConfigConst::NS_KEY => "courses.doctoral",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "staff.prof",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     },
     {
       SolrConfigConst::NS_KEY => "staff.admin",
       SolrConfigConst::COLL_FIELD_KEY => { "address" => 1 }
     }
    ]
  }
end

