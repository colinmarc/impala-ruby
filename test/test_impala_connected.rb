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

  describe 'basic tests' do
    it 'can connect' do
      assert_instance_of(Impala::Connection, @connection)
      assert(@connection.open?, "the connection should be open")
    end

    it 'can refresh the catalog' do
      @connection.refresh
    end

    it 'can refresh metadata' do
      @connection.query('invalidate metadata')
    end

    it 'can run a basic query' do
      ret = @connection.query('SELECT "foo" AS foo')
      assert_equal([{:foo=>'foo'}], ret, "the result should be a list of hashes")
    end

    it 'can handle boolean values' do
      ret = @connection.query('SELECT TRUE AS foo')
      assert_equal([{:foo=>true}], ret, "the result should be a bool")
    end

    it 'can handle double values' do
      ret = @connection.query('SELECT 1.23 AS foo')
      assert_equal([{:foo=>1.23}], ret, "the result should be a float")
    end

    it 'can handle float values' do
      ret = @connection.query('SELECT CAST(1.23 AS float) as foo')
      assert_instance_of(Float, ret.first[:foo], "the result should be a float")
    end

    it 'can handle timestamp values' do
      ret = @connection.query('SELECT NOW() AS foo')
      assert_instance_of(Time, ret.first[:foo], "the result should be a timestamp")
    end

    it 'can handle null values' do
      ret = @connection.query('SELECT NULL AS nothing')
      assert_equal(nil, ret.first[:nothing], "the result should be nil")
    end

    it 'can handle the string "NULL"' do
      ret = @connection.query('SELECT "NULL" as something')
      assert_instance_of(String, ret.first[:something], "the result should be a string")
    end

    it 'can successfully refresh the metadata store' do
      ret = @connection.refresh
    end
  end

  describe 'with a test database' do
    before do
      @database = '_impala_ruby_test'
      @connection.query("CREATE DATABASE IF NOT EXISTS #{@database}")
    end

    after do
      @connection.query("DROP DATABASE IF EXISTS #{@database}") if @connection
    end

    it 'can use the database' do
      @connection.query("USE #{@database}")
      @connection.query("USE default")
    end

    describe 'and a test table' do
      before do
        @table = "#{@database}.foobar"
        @connection.query("CREATE TABLE #{@table} (i INT)")
      end

      after do
        @connection.query("DROP TABLE #{@table}") if @connection
      end

      it 'deals with empty tables correctly when using #query' do
        res = @connection.query("SELECT * FROM #{@table}")
        assert_equal([], res, "the result set should be empty")
      end

      it 'deals with empty tables correctly when using a cursor' do
        cursor = @connection.execute("SELECT * FROM #{@table}")
        assert_equal(false, cursor.has_more?, "has_more? should be false")
        assert_nil(cursor.fetch_row, "calls to fetch_row should be nil")
      end

      describe 'with data' do
        before do
          @connection.query("INSERT INTO #{@table} (i) SELECT 1")
          @connection.query("INSERT INTO #{@table} (i) SELECT 1")
          @connection.query("INSERT INTO #{@table} (i) SELECT 1")
        end
        
        it 'can handle the keywoard "with"' do
          res = @connection.query("with bar as (select * from #{@table}) select * from bar")
          assert_equal([{:i => 1}, {:i => 1}, {:i => 1}], res)
        end

        it 'can insert into the table' do
          @connection.query("INSERT INTO #{@table} (i) SELECT 2")
        end

        it 'can select from the table using #query' do
          res = @connection.query("SELECT * FROM #{@table}")
          assert_equal([{:i => 1}, {:i => 1}, {:i => 1}], res)
        end

        it 'can create a cursor and fetch one row at a time' do
          cursor = @connection.execute("SELECT * FROM #{@table}")
          assert_instance_of(Impala::Cursor, cursor, "the result should be a cursor")

          3.times do
            row = cursor.fetch_row
            assert_equal({:i=>1}, row, "the row should be a hash with the correct result")
          end

          assert_equal(false, cursor.has_more?, "has_more? should be false")
          assert_nil(cursor.fetch_row, "subsequent calls to fetch_row should be nil")
        end

        it 'can use a cursor to deal with lots of data' do
          10.times { @connection.query("INSERT INTO #{@table} SELECT * FROM #{@table}") }
          @connection.query("INSERT INTO #{@table} (i) SELECT 1")
          count = @connection.query("SELECT COUNT(*) as n from #{@table}")[0][:n]
          assert(count > Impala::Cursor::BUFFER_SIZE) # otherwise the test is pointless

          cursor = @connection.execute("SELECT * FROM #{@table}")
          assert_instance_of(Impala::Cursor, cursor, "the result should be a cursor")

          # fetch one to fill the buffer
          row = cursor.fetch_row
          assert_equal({:i=>1}, row, "the row should be a hash with the correct result")

          buffer_size = cursor.instance_variable_get('@row_buffer').size
          assert_equal(Impala::Cursor::BUFFER_SIZE - 1, buffer_size, "it should only buffer #{Impala::Cursor::BUFFER_SIZE} rows into memory")

          (count - 1).times do
            row = cursor.fetch_row
            assert_equal({:i=>1}, row, "the row should be a hash with the correct result")
          end

          assert_equal(false, cursor.has_more?, "has_more? should be false")
          assert_nil(cursor.fetch_row, "subsequent calls to fetch_row should be nil")
        end
      end
      
    end
  end
end
