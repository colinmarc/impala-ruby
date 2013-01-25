module Impala
  class Connection
    SLEEP_INTERVAL = 0.1

    def initialize(host='localhost', port=21000)
      @host = host
      @port = port
      @connected = false
      open
    end

    def open
      return if @connected

      socket = Thrift::Socket.new(@host, @port)

      @transport = Thrift::BufferedTransport.new(socket)
      @transport.open

      proto = Thrift::BinaryProtocol.new(@transport)
      @service = Protocol::ImpalaService::Client.new(proto)
      @connected = true
    end

    def close
      @transport.close
      @connected = false
    end

    def open?
      @connected
    end

    def query(raw_query)
      execute(raw_query).fetch_all
    end

    def execute(raw_query)
      raise ConnectionError.new("Connection closed") unless open?

      query = sanitize_query(raw_query)
      handle = send_query(query)

      wait_for_result(handle)
      Cursor.new(handle, @service)
    end

    private

    def sanitize_query(raw_query)
      words = raw_query.split
      raise InvalidQueryError.new("Empty query") if words.empty?

      command = words.first.downcase
      if !KNOWN_COMMANDS.include?(command)
        raise InvalidQueryError.new("Unrecognized command: '#{words.first}'")
      end

      ([command] + words[1..-1]).join(' ')
    end

    def send_query(sanitized_query)
      query = Protocol::Beeswax::Query.new
      query.query = sanitized_query

      @service.query(query)
    end

    def wait_for_result(handle)
      begin
        #TODO select here, or something
        while true
          state = @service.get_state(handle)
          if state == Protocol::Beeswax::QueryState::FINISHED
            break
          elsif state == Protocol::Beeswax::QueryState::EXCEPTION
            close_handle(handle)
            raise ConnectionError.new("The query was aborted")
          end

          sleep(SLEEP_INTERVAL)
        end
      rescue Interrupt
        close_handle(handle)
        raise
      end
    end

    def close_handle(handle)
      @service.close(handle)
    end
  end
end