this_dir = File.expand_path(File.dirname(__FILE__))
gen_dir = File.join(this_dir, 'impala', 'protocol')
$LOAD_PATH.unshift(gen_dir) unless $LOAD_PATH.include?(gen_dir)

require 'impala/version'
require 'impala/protocol'
require 'impala/connection'

module Impala
  KNOWN_COMMANDS = ['select']
  class InvalidQueryException < Exception; end

  def self.with_connection(host='localhost', port=21000)
    connection = Connection.new
    yield connection
    connection.close

    connection.last_result
  end
end
