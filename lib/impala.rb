
# the generated ruby files use a relative require, so we need to add the
# generated directory to $LOAD_PATH
this_dir = File.expand_path(File.dirname(__FILE__))
gen_dir = File.join(this_dir, 'impala/protocol')
$LOAD_PATH.push(gen_dir) unless $LOAD_PATH.include?(gen_dir)

require 'impala/version'

require 'thrift'
require 'impala/protocol'
require 'impala/cursor'
require 'impala/connection'

module Impala
  KNOWN_COMMANDS = ['select', 'show', 'describe', 'use']
  DEFAULT_HOST = 'localhost'
  DEFAULT_PORT = 21000
  class InvalidQueryError < StandardError; end
  class ConnectionError < StandardError; end
  class CursorError < StandardError; end
  class ParsingError < StandardError; end

  def self.connect(host=DEFAULT_HOST, port=DEFAULT_PORT)
    connection = Connection.new(host, port)

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
