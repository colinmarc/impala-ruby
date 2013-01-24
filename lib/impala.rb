
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
  class InvalidQueryException < Exception; end

  def self.with_connection(host='localhost', port=21000)
    connection = Connection.new(host, port)
    yield connection
    connection.close

    connection.last_result
  end
end
