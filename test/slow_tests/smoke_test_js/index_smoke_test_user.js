load("../../../solr-plugin.js");

MSolr.connect("http://localhost:8983/solr/");
db.getSiblingDB("smoke_test").user.solrIndex();

