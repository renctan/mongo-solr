# A simple class for instantiating a factory object that can create an instance of
# a given type and partially apply arguments.
module MongoSolr
  class Factory
    attr_reader :klass
    alias_method :type, :klass

    attr_reader :partial_args

    # @param klass [Class, MongoSolr::Factory] A class object that can be instantiated. If
    #   an instance of MongoSolr::Factory is given the new args will be concatenated to the
    #   partial arguments of the given factory object.
    # @param args [Splat] Set of arguments that will be partially applied to the constructor
    #   of klass.
    def initialize(klass, *args)
      if klass.is_a? MongoSolr::Factory then
        @partial_args = klass.partial_args
        @partial_args.concat(args)
        @klass = klass.type
      else
        @klass = klass
        @partial_args = *args
      end
    end

    def create(*args, &block)
      complete_args = @partial_args
      complete_args.concat(args)
      @klass.new(*complete_args, &block)
    end
  end
end

