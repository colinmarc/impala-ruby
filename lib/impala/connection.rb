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
    FETCH_SIZE = 1024

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

    def execute(query, opts={})
      words = query.split

      unless KNOWN_COMMANDS.include?(words.first)
        raise InvalidQueryException.new("Unrecognized command: #{words.first}")
      end

      query = create_query(query, opts)
      handle = @service.query(query)

      @last_result = get_results(handle)
    end

    def last_result
      @last_result
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

    def get_results(handle)
      #TODO select here, or something
      while true
        state = @service.get_state(handle)
        if state == Protocol::Beeswax::QueryState::FINISHED
          return fetch_results(handle)
        elsif state == Protocol::Beeswax::QueryState::EXCEPTION
          close_handle(handle)
          raise "something went wrong" #TODO
        end

        sleep(SLEEP_INTERVAL)
      end
    end

    def fetch_results(handle)
      metadata = @service.get_results_metadata(handle)
      result_rows = []

      while true
        res = @service.fetch(handle, false, FETCH_SIZE)
        rows = res.data.map { |raw| parse_row(raw, metadata) }
        result_rows += rows

        break unless res.has_more
      end

      result_rows
    end

    def parse_row(raw, metadata)
      row = {}
      fields = raw.split(metadata.delim)

      fields.zip(metadata.schema.fieldSchemas).each do |raw_value, schema|
        value = convert_raw_value(raw_value, schema)
        row[schema.name.to_sym] = value
      end

      row
    end


    def convert_raw_value(value, schema)
      case schema.type
      when 'string'
        value
      when 'int', 'bigint'
        value.to_i
      else
        raise "Unknown type: #{schema.type}" #TODO
      end
    end

    def close_handle(handle)
      @service.close(handle)
    end
  end
end