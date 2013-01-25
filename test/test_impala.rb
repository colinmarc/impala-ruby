require 'impala'
require 'test-unit'
require 'shoulda'
require 'mocha'

class TestImpala < Test::Unit::TestCase
  include Mocha

  context 'Impala.connect' do
    setup do
      Thrift::Socket.expects(:new).with('host', 12345)
      Thrift::BufferedTransport.expects(:new).once.returns(stub(:open => nil))
      Thrift::BinaryProtocol.expects(:new).once
      Impala::Protocol::ImpalaService::Client.expects(:new).once
    end

    should 'return an open connection when passed a block' do
      connection = Impala.connect('host', 12345)
      assert_equal(Impala::Connection, connection.class)
      assert_equal(true, connection.open?)
    end

    should 'return the results of the query when given a block with a query, and then close tho connection' do
      Impala::Connection.any_instance.stubs(:query => 'result')
      Impala::Connection.any_instance.expects(:close).once

      ret = Impala.connect('host', 12345) do |conn|
        conn.query('query')
      end

      assert_equal('result', ret)
    end
  end
end