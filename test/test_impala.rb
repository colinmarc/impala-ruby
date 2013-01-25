require 'impala'
require 'minitest/autorun'
require 'mocha'

describe 'Impala.connect' do
  before do
    Thrift::Socket.expects(:new).with('host', 12345)
    Thrift::BufferedTransport.expects(:new).once.returns(stub(:open => nil))
    Thrift::BinaryProtocol.expects(:new).once
    Impala::Protocol::ImpalaService::Client.expects(:new).once
  end

  it 'should return an open connection when passed a block' do
    connection = Impala.connect('host', 12345)
    assert_equal(Impala::Connection, connection.class)
    assert_equal(true, connection.open?)
  end

  it 'should return the results of the query when given a block with a query, and then close tho connection' do
    Impala::Connection.any_instance.stubs(:query => 'result')
    Impala::Connection.any_instance.expects(:close).once

    ret = Impala.connect('host', 12345) do |conn|
      conn.query('query')
    end

    assert_equal('result', ret)
  end
end

describe Impala::Connection do
  describe '#sanitize_query' do
    before do
      Impala::Connection.any_instance.stubs(:open)
      @connection = Impala::Connection.new('test', 1234)
    end

    it 'should downcase the command but nothing else' do
      query = 'SELECT blah FROM Blah'
      assert_equal('select blah FROM Blah', @connection.send(:sanitize_query, query))
    end

    it 'should reject empty or invalid queries' do
      assert_raises(Impala::InvalidQueryError) { @connection.send(:sanitize_query, '')}
      assert_raises(Impala::InvalidQueryError) { @connection.send(:sanitize_query, 'HERRO herro herro')}
    end
  end

  describe '#wait_for_result' do
    before do
      Impala::Connection.any_instance.stubs(:open)
      @connection = Impala::Connection.new('test', 1234)
      @service = stub(:get_state => nil)
      @connection.instance_variable_set('@service', @service)
    end

    it 'should close the handle if an exception is raised' do
      handle = stub()
      @service.expects(:close).with(handle).once
      @service.expects(:get_state).raises(StandardError)
      assert_raises(StandardError) { @connection.send(:wait_for_result, handle) }
    end
  end
end