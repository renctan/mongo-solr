# A simple class for instantiating a builder object that can create an instance of
# a given type and partially apply arguments.
module MongoSolr
  class ObjectBuilder
    attr_reader :klass
    alias_method :type, :klass

    attr_reader :partial_args

    # @param klass [Class, MongoSolr::ObjectBuilder] A class object that can be instantiated. If
    #   an instance of MongoSolr::ObjectBuilder is given the new args will be concatenated to the
    #   partial arguments of the given builder object.
    # @param args [Splat] Set of arguments that will be partially applied to the constructor
    #   of klass.
    def initialize(klass, *args)
      if klass.is_a? MongoSolr::ObjectBuilder then
        @partial_args = klass.partial_args + args
        @klass = klass.type
      else
        @klass = klass
        @partial_args = args
      end
    end

    # Creates a new instance of the initialized class type by combining the partially
    # initialized arguments with the currently given arguments and block
    #
    # @param args [Splat] The remaining arguments to apply after the ones given at the
    #   constructor.
    # @param block [Proc] A block to pass to the constructor.
    #
    # @return [Object] the new object created using the contructor of the initialized class.
    def create(*args, &block)
      complete_args = @partial_args + args
      @klass.new(*complete_args, &block)
    end
  end
end

