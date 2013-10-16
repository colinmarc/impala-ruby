module Impala
  # This object represents a connection to an Impala server. It can be used to
  # perform queries on the database.
  class Connection
    SLEEP_INTERVAL = 0.1

    # Don't instantiate Connections directly; instead, use {Impala.connect}.
    def initialize(host, port)
      @host = host
      @port = port
      @connected = false
      open
    end

    def inspect
      "#<#{self.class} #{@host}:#{@port}#{open? ? '' : ' (DISCONNECTED)'}>"
    end

    # Open the connection if it's currently closed.
    def open
      return if @connected

      socket = Thrift::Socket.new(@host, @port)

      @transport = Thrift::BufferedTransport.new(socket)
      @transport.open

      proto = Thrift::BinaryProtocol.new(@transport)
      @service = Protocol::ImpalaService::Client.new(proto)
      @connected = true
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

    # Refresh the metadata store
    def refresh
      raise ConnectionError.new("Connection closed") unless open?
      @service.ResetCatalog
    end

    # Perform a query and return all the results. This will
    # load the entire result set into memory, so if you're dealing with lots
    # of rows, {#execute} may work better.
    # @param [String] query the query you want to run
    # @param [Hash] opt the option such as "hadoop_user" and "configuration"
    # @return [Array<Hash>] an array of hashes, one for each row.
    def query(raw_query, opt = {})
      execute(raw_query, opt).fetch_all
    end

    # Perform a query and return a cursor for iterating over the results.
    # @param [String] query the query you want to run
    # @param [Hash] opt the option such as "hadoop_user" and "configuration"
    # @return [Cursor] a cursor for the result rows
    def execute(raw_query, opt = {})
      raise ConnectionError.new("Connection closed") unless open?

      query = sanitize_query(raw_query)
      handle = send_query(query, opt)

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

    def send_query(sanitized_query, opt = {})
      query = Protocol::Beeswax::Query.new
      query.query = sanitized_query
      query.hadoop_user = opt[:hadoop_user] if opt[:hadoop_user]
      query.configuration = opt[:configuration] if opt[:configuration]
      @service.query(query)
    end

    def wait_for_result(handle)
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
    rescue
      close_handle(handle)
      raise
    end

    def close_handle(handle)
      @service.close(handle)
    end
  end
end
