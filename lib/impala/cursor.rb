module Impala
  class Cursor
    include Enumerable

    def initialize(handle, service, buffer_length=1024)
      @handle = handle
      @service = service
      @metadata = @service.get_results_metadata(@handle)

      @buffer_length = buffer_length
      @row_buffer = []
      @done = false
    end

    def each
      while row = fetch_row
        yield row
      end
    end

    def fetch_row
      if @row_buffer.empty?
        if @done
          return nil
        else
          fetch_more
        end
      end

      @row_buffer.shift
    end

    def fetch_all
      self.to_a
    end

    private

    def fetch_more
      return if @done

      res = @service.fetch(@handle, false, @buffer_length)
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
      case schema.type
      when 'string'
        value
      when 'tinyint', 'int', 'bigint'
        value.to_i
      else
        raise "Unknown type: #{schema.type}" #TODO
      end
    end
  end
end