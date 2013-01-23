module Impala
  class Connection
    def initialize(host='localhost', port=21000)
      @host = host
      @port = port

      self.open
    end

    def close
    end

    def execute(query)
      words = query.split

      unless KNOWN_COMMANDS.include?(words.first)
        raise InvalidQueryException.new("Unrecognized command: #{words.first}")
      end

      @last_result = 1
    end

    def last_result
      @last_result
    end

    private

    def open
    end
  end
end