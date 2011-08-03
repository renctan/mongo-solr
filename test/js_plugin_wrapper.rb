require File.expand_path("../proj", __FILE__)

# A simple class for executing Javascript codes in the context of the Mongo shell.
class JSPluginWrapper
  SOLR_PLUGIN_JS_FILE = "#{PROJ_SRC_PATH}/../solr-plugin.js"
  SOLR_LOC = "http://localhost:8983/solr/"

  # @param host [String]
  # @param port
  def initialize(host, port)
    @cmd = "mongo --host #{host} --port #{port}" +
      " --eval \"load(\\\"#{SOLR_PLUGIN_JS_FILE}\\\");%s\""
  end

  # Execute a javascript source code.
  #
  # @param js_code [String] The javascript source code to execute.
  #
  # @return [String] The output of the javascript code.
  def eval(js_code)
    full_cmd = @cmd % [escape_js(js_code)]
    `#{full_cmd}`
  end

  # Sets the collection for indexing to Solr.
  #
  # @param db [String] The database name of the collection.
  # @param coll [String] The collection name.
  def index_to_solr(db, coll = "")
    if coll.empty? then
      index_line = "db.getSiblingDB(\"#{db}\").solrIndex();"
    else
      index_line = "db.getSiblingDB(\"#{db}\").#{coll}.solrIndex();"
    end

    code = <<JAVASCRIPT
    MSolr.connect("#{SOLR_LOC}");
    #{index_line}
JAVASCRIPT

    eval(code)
  end

  private

  # Escapes the given string to correctly execute when passed to the eval option of
  # the mongo shell program.
  #
  # @param code [String] the original javascript code.
  #
  # @return [String] the escaped string.
  def escape_js(code)
    code.gsub(/\"/, "\\\"")
  end
end

