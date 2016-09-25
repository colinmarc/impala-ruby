
# the generated ruby files use a relative require, so we need to add the
# generated directory to $LOAD_PATH
this_dir = File.expand_path(File.dirname(__FILE__))
gen_dir = File.join(this_dir, 'impala/protocol')
$LOAD_PATH.push(gen_dir) unless $LOAD_PATH.include?(gen_dir)

require 'impala/version'

require 'thrift'
require 'time'
require 'impala/protocol'
require 'impala/sasl_transport'
require 'impala/cursor'
require 'impala/connection'

module Impala
  DEFAULT_HOST = 'localhost'
  DEFAULT_PORT = 21000
  class InvalidQueryError < StandardError; end
  class ConnectionError < StandardError; end
  class CursorError < StandardError; end
  class ParsingError < StandardError; end

  # Connect to an Impala server. If a block is given, it will close the
  #   connection after yielding the connection to the block.
  # @param [String] host the hostname or IP address of the Impala server
  # @param [int] port the port that the Impala server is listening on
  # @param [Hash] options connection options
  # @option options [int] :timeout the timeout in seconds to use when connecting
  # @option options [Hash] :sasl if present, used to connect with SASL PLAIN
  #   authentication. Should have two properties:
  #   - *:username* (String)
  #   - *:password* (String)
  # @option options [Hash] :kerberos if present, used to connect with SASL
  #   GSSAPI authentication using whatever context is available. Should have two
  #   properties:
  #   - *:host* (String)
  #   - *:provider* (String)
  # @yieldparam [Connection] conn the open connection. Will be closed once the block
  #    finishes
  # @return [Connection] the open connection, or, if a block is
  #    passed, the return value of the block
  def self.connect(host=DEFAULT_HOST, port=DEFAULT_PORT, options={})
    connection = Connection.new(host, port, options)

    if block_given?
      begin
        ret = yield connection
      ensure
        connection.close
      end
    else
      ret = connection
    end

    ret
  end
end
