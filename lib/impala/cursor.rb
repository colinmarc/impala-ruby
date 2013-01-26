module Impala
  # Cursors are used to iterate over result sets without loading them all
  # into memory at once. This can be useful if you're dealing with lots of
  # rows. It implements Enumerable, so you can use each/select/map/etc.
  class Cursor
    include Enumerable

    def initialize(handle, service, buffer_length=1024)
      @handle = handle
      @service = service
      @metadata = @service.get_results_metadata(@handle)

      @buffer_length = buffer_length
      @row_buffer = []

      @done = false
      @open = true
    end

    def inspect
      "#<#{self.class}#{open? ? '' : ' (CLOSED)'}>"
    end

    def each
      while row = fetch_row
        yield row
      end
    end

    # Returns the next available row as a hash, or nil if there are none left.
    # @return [Hash, nil] the next available row, or nil if there are none
    #    left
    # @see #fetch_all
    def fetch_row
      raise CursorError.new("Cursor has expired or been closed") unless @open

      if @row_buffer.empty?
        if @done
          return nil
        else
          fetch_more
        end
      end

      @row_buffer.shift
    end

    # Returns all the remaining rows in the result set.
    # @return [Array<Hash>] the remaining rows in the result set
    # @see #fetch_one
    def fetch_all
      self.to_a
    end

    # Close the cursor on the remote server. Once a cursor is closed, you
    # can no longer fetch any rows from it.
    def close
      @open = false
      @service.close(@handle)
    end

    # Returns true if the cursor is still open.
    def open?
      @open
    end

    # Returns true if there are any more rows to fetch.
    def has_more?
      !@done || !@row_buffer.empty?
    end

    private

    def fetch_more
      return if @done

      begin
        res = @service.fetch(@handle, false, @buffer_length)
      rescue Protocol::Beeswax::BeeswaxException => e
        @closed = true
        raise CursorError.new("Cursor has expired or been closed")
      end

      rows = res.data.map { |raw| parse_row(raw) }
      @row_buffer.concat(rows)
      @done = true unless res.has_more
    end

    def parse_row(raw)
      row = {}
      fields = raw.split(@metadata.delim)

      fields.zip(@metadata.schema.fieldSchemas).each do |raw_value, schema|
        value = convert_raw_value(raw_value, schema)
        row[schema.name.to_sym] = value
      end

      row
    end

    def convert_raw_value(value, schema)
      return nil if value == 'NULL'

      case schema.type
      when 'string'
        value
      when 'boolean'
        if value == 'true'
          true
        elsif value == 'false'
          false
        else
          raise ParsingError.new("Invalid value for boolean: #{value}")
        end
      when 'tinyint', 'int', 'bigint'
        value.to_i
      when 'double'
        value.to_f
      else
        raise ParsingError.new("Unknown type: #{schema.type}")
      end
    end
  end
end