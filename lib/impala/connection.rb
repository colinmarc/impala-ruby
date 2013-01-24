module Impala
  class Connection
    #TODO do something smarter for config here
    #TODO figure out what these do
    QUERY_OPTS = {
      "DISABLE_CODEGEN"=>"false",
      "MAX_IO_BUFFERS"=>"0",
      "ABORT_ON_ERROR"=>"false",
      "BATCH_SIZE"=>"0",
      "NUM_SCANNER_THREADS"=>"0",
      "ALLOW_UNSUPPORTED_FORMATS"=>"false",
      "MAX_ERRORS"=>"0",
      "NUM_NODES"=>"0",
      "DEFAULT_ORDER_BY_LIMIT"=>"-1",
      "MAX_SCAN_RANGE_LENGTH"=>"0"
    }
    QUERY_CONFIG = QUERY_OPTS.map { |k,v| "#{k}=#{v}" }
    SLEEP_INTERVAL = 0.5

    def initialize(host='localhost', port=21000)
      @host = host
      @port = port
      @connected = false
      open
    end

    def open
      return if @connected

      socket = Thrift::Socket.new(@host, @port)

      transport = Thrift::BufferedTransport.new(socket)
      transport.open

      proto = Thrift::BinaryProtocol.new(transport)
      @service = Protocol::ImpalaService::Client.new(proto)
      @connected = true
    end

    def close
      #TODO
    end

    def open?
      @connected
    end

    def query(raw_query, opts={})
      execute(raw_query, opts).to_a
    end

    def execute(raw_query, opts={})
      words = raw_query.split

      unless KNOWN_COMMANDS.include?(words.first.downcase)
        raise InvalidQueryException.new("Unrecognized command: '#{words.first}'")
      end

      query = create_query(raw_query.downcase, opts)
      handle = @service.query(query)

      create_cursor(handle)
    end

    private

    def sanitize_query(raw)
      #TODO?
      raw
    end

    def create_query(raw_query, opts)
      query = Protocol::Beeswax::Query.new
      query.query = sanitize_query(raw_query)
      query.configuration = QUERY_CONFIG

      query
    end

    def create_cursor(handle)
      #TODO select here, or something
      while true
        state = @service.get_state(handle)
        if state == Protocol::Beeswax::QueryState::FINISHED
          return Cursor.new(handle, @service)
        elsif state == Protocol::Beeswax::QueryState::EXCEPTION
          close_handle(handle)
          raise "something went wrong" #TODO
        end

        sleep(SLEEP_INTERVAL)
      end
    end

    def close_handle(handle)
      @service.close(handle)
    end
  end
end