require 'impala'
require 'minitest/autorun'
require 'mocha/setup'

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
    end
  end

  describe '#check_result' do
    before do
      Impala::Connection.any_instance.stubs(:open)
      @connection = Impala::Connection.new('test', 1234)
      @service = stub(:get_state => nil)
      @connection.instance_variable_set('@service', @service)
    end

    it 'should close the handle if an exception is raised, and then re-raise' do
      handle = stub()
      @service.expects(:close).with(handle).once
      @service.expects(:get_state).raises(StandardError)
      assert_raises(StandardError) { @connection.send(:check_result, handle) }
    end
  end

  describe '#execute' do
    before do
      Impala::Connection.any_instance.stubs(:open)
      Impala::Cursor.stubs(:new)
      @connection = Impala::Connection.new('test', 1234)
      @connection.stubs(:open? => true, :sanitize_query => 'sanitized_query', :check_result => nil)
    end

    it 'should call Protocol::ImpalaService::Client#executeAndWait with the sanitized query' do
      query = Impala::Protocol::Beeswax::Query.new
      query.query = 'sanitized_query'
      query.configuration = []

      @service = stub()
      @service.expects(:executeAndWait).with(query, Impala::Connection::LOG_CONTEXT_ID).once
      @connection.instance_variable_set('@service', @service)

      @connection.execute('query')
    end

    it 'should call Protocol::ImpalaService::Client#executeAndWait with the hadoop_user and configuration if passed as parameter' do
      query = Impala::Protocol::Beeswax::Query.new
      query.query = 'sanitized_query'
      query.hadoop_user = 'impala'
      query.configuration = %w|NUM_SCANNER_THREADS=8 MEM_LIMIT=3221225472|

      @service = stub()
      @service.expects(:executeAndWait).with(query, Impala::Connection::LOG_CONTEXT_ID).once
      @connection.instance_variable_set('@service', @service)

      opt = {
        :user => 'impala',
        :num_scanner_threads => 8,
        :mem_limit => 3221225472
      }
      @connection.execute('query', opt)
    end
  end
end
