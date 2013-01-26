require 'impala'
require 'minitest/autorun'
require 'mocha'

# these are tests that require an available Impala server. To run them,
# declare a IMPALA_SERVER env var, e.g. `IMPALA_SERVER=localhost:21000 rake`
IMPALA_SERVER = ENV['IMPALA_SERVER']

def connect
  parts = IMPALA_SERVER.split(':')
  if parts.length != 2 || parts.any? { |p| p.empty? }
    raise "Invalid IMPALA_SERVER: #{IMPALA_SERVER}"
  end

  host, port = parts
  Impala.connect(host, port)
end

describe 'connected tests' do
  before do
    skip unless IMPALA_SERVER
    @connection = connect
  end

  it 'can successfully connect' do
    assert_instance_of(Impala::Connection, @connection)
    assert(@connection.open?, "the connection should be open")
  end

  it 'can run a basic query' do
    ret = @connection.query('SELECT 1 AS a')
    assert_equal([{:a=>1}], ret, "the result should be a list of hashes")
  end

  # TODO: this test sucks because there's no way to get multiple records
  # with a literal select. perhaps there should be importable test data?
  it 'can get a cursor and fetch one row at a time' do
    cursor = @connection.execute('SELECT 1 AS a')
    assert_instance_of(Impala::Cursor, cursor, "the result should be a cursor")

    row = cursor.fetch_row
    assert_equal({:a=>1}, row, "the row should be a hash")

    assert_equal(false, cursor.has_more?, "has_more? should be false")
    assert_nil(cursor.fetch_row, "subsequent calls to fetch_row should be nil")
  end
end