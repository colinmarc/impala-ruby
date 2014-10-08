module Impala
  # This object represents a connection to an Impala server. It can be used to
  # perform queries on the database.
  class Connection
    LOG_CONTEXT_ID = "impala-ruby"

    attr_accessor :host, :port

    # Don't instantiate Connections directly; instead, use {Impala.connect}.
    def initialize(host, port, options={})
      @host = host
      @port = port
      @connected = false
      options[:transport] ||= :buffered
      @options = options
      open
    end

    def inspect
      "#<#{self.class} #{@host}:#{@port}#{open? ? '' : ' (DISCONNECTED)'}>"
    end

    # Open the connection if it's currently closed.
    def open
      return if @connected

      @transport = thrift_transport(host, port)
      @transport.open

      proto = Thrift::BinaryProtocol.new(@transport)
      @service = Protocol::ImpalaService::Client.new(proto)
      @connected = true
    end

    def thrift_transport(server, port)
      case @options[:transport]
      when :buffered
        return Thrift::BufferedTransport.new(thrift_socket(server, port, @options[:timeout]))
      when :sasl
        return Thrift::ImpalaSaslClientTransport.new(thrift_socket(server, port, @options[:timeout]),
                                               parse_sasl_params(@options[:sasl_params]))
      else
        raise "Unrecognised transport type '#{@options[:transport]}'"
      end
    end

    def thrift_socket(server, port, timeout)
      socket = Thrift::Socket.new(server, port)
      socket.timeout = timeout
      socket
    end

    # Processes SASL connection params and returns a hash with symbol keys or a nil
    def parse_sasl_params(sasl_params)
      # Symbilize keys in a hash
      if sasl_params.kind_of?(Hash)
        return sasl_params.inject({}) do |memo,(k,v)|
          memo[k.to_sym] = v;
          memo
        end
      end
      return nil
    end

    # Close this connection. It can still be reopened with {#open}.
    def close
      return unless @connected

      @transport.close
      @connected = false
    end

    # Returns true if the connection is currently open.
    def open?
      @connected
    end

    # Refresh the metadata store.
    def refresh
      raise ConnectionError.new("Connection closed") unless open?
      @service.ResetCatalog
    end

    # Perform a query and return all the results. This will
    # load the entire result set into memory, so if you're dealing with lots
    # of rows, {#execute} may work better.
    # @param [String] query the query you want to run
    # @param [Hash] query_options the options to set user and configuration
    #   except for :user, see TImpalaQueryOptions in ImpalaService.thrift
    # @option query_options [String] :user the user runs the query
    # @return [Array<Hash>] an array of hashes, one for each row.
    def query(raw_query, query_options = {})
      execute(raw_query, query_options).fetch_all
    end

    # Perform a query and return a cursor for iterating over the results.
    # @param [String] query the query you want to run
    # @param [Hash] query_options the options to set user and configuration
    #   except for :user, see TImpalaQueryOptions in ImpalaService.thrift
    # @option query_options [String] :user the user runs the query
    # @return [Cursor] a cursor for the result rows
    def execute(raw_query, query_options = {})
      raise ConnectionError.new("Connection closed") unless open?

      query = sanitize_query(raw_query)
      handle = send_query(query, query_options)

      check_result(handle)
      Cursor.new(handle, @service)
    end

    private

    def sanitize_query(raw_query)
      words = raw_query.split
      raise InvalidQueryError.new("Empty query") if words.empty?

      command = words.first.downcase
      ([command] + words[1..-1]).join(' ')
    end

    def send_query(sanitized_query, query_options)
      query = Protocol::Beeswax::Query.new
      query.query = sanitized_query

      query.hadoop_user = query_options.delete(:user) if query_options[:user]
      query.configuration = query_options.map do |key, value|
        "#{key.upcase}=#{value}"
      end

      @service.executeAndWait(query, LOG_CONTEXT_ID)
    end

    def check_result(handle)
      state = @service.get_state(handle)
      if state == Protocol::Beeswax::QueryState::EXCEPTION
        close_handle(handle)
        raise ConnectionError.new("The query was aborted")
      end
    rescue
      close_handle(handle)
      raise
    end

    def close_handle(handle)
      @service.close(handle)
    end
  end
end
