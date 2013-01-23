this_dir = File.expand_path(File.dirname(__FILE__))
gen_dir = File.join(this_dir, 'impala', 'protocol')
$LOAD_PATH.unshift(gen_dir) unless $LOAD_PATH.include?(gen_dir)

require 'impala/version'
require 'impala/protocol'

module Impala
end
