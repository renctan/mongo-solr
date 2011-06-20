require "mongo"

module MongoSolr
  class DocumentTransform
    DEFAULT_ARRAY_SEPARATOR = "_"
    DEFAULT_HASH_SEPARATOR = "_"

    # Converts a given document into a hash structure with only one level of nesting by
    # collapsing the whole access path to all leaf elements into a single string that
    # serves as the key to that element.
    #
    # @param doc [Hash, BSON::OrderedHash] The document to convert
    # @param array_separator [String] ("_") The string to use when delimiting the name for 
    #   the index of an array
    # @param hash_separator [String] ("_") The string to use when delimiting the name for 
    #   the key of a hash
    #
    # @return [Hash] The new hash with flattened structure
    #
    # Example:
    #
    # DocumentTransform.translate_doc({ "foo" => %w[Hello world],
    #                                   "bar" => { "mirror" => "rab" }},
    #                                  "@", "/")
    #  # => { "foo@0" => "Hello", "foo@1" => "world", "bar/mirror" => "rab" }
    def self.translate_doc(doc, array_separator = DEFAULT_ARRAY_SEPARATOR,
                           hash_separator = DEFAULT_HASH_SEPARATOR)
      flatten_hash("", doc, array_separator, hash_separator)
    end

    ############################################################################
    private

    # Flattens an array.
    #
    # @param key_prefix [String] The key of the parent element
    # @param array [Array] The array to flatten
    # @param array_separator [String] ("_") The string to use when delimiting the name for 
    #   the index of an array
    # @param hash_separator [String] ("_") The string to use when delimiting the name for 
    #   the key of a hash
    #
    # @return [Hash] The new hash with flattened structure
    def self.flatten_array(key_prefix, array, array_separator, hash_separator)
      flattened_hash = {}

      array.each_with_index do |elem, index|
        new_key = "#{key_prefix}#{array_separator}#{index}"
        flattened_nested_hash = case elem
                                when Hash, BSON::OrderedHash then
                                  flatten_hash(new_key, elem, array_separator, hash_separator)
                                when Array then
                                  flatten_array(new_key, elem, array_separator, hash_separator)
                                else
                                  { new_key => elem }
                                end

        flattened_hash.merge!(flattened_nested_hash)
      end

      return flattened_hash
    end

    # Flattens a hash.
    #
    # @param key_prefix [String] The key of the parent element
    # @param hash [Hash, BSON::OrderedHash] A hash to convert
    # @param array_separator [String] ("_") The string to use when delimiting the name for 
    #   the index of an array
    # @param hash_separator [String] ("_") The string to use when delimiting the name for 
    #   the key of a hash
    #
    # @return [Hash] The new hash with flattened structure
    def self.flatten_hash(key_prefix, hash, array_separator, hash_separator)
      flattened_hash = {}

      hash.each do |key, value|
        new_key = (key_prefix.empty? ? key : "#{key_prefix}#{hash_separator}#{key}")
        flattened_nested_hash = case value
                                when Hash, BSON::OrderedHash then
                                  flatten_hash(new_key, value, array_separator, hash_separator)
                                when Array then
                                  flatten_array(new_key, value, array_separator, hash_separator)
                                else
                                  { new_key => value }
                                end

        flattened_hash.merge!(flattened_nested_hash)
      end

      return flattened_hash
    end
  end
end

