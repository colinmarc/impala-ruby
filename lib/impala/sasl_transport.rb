require 'gssapi'

module Impala
  class SASLTransport < Thrift::FramedTransport
    STATUS_BYTES = 1
    PAYLOAD_LENGTH_BYTES = 4
    NEGOTIATION_STATUS = {
      START:    0x01,
      OK:       0x02,
      BAD:      0x03,
      ERROR:    0x04,
      COMPLETE: 0x05
    }

    def initialize(transport, mechanism, options={})
      super(transport)
      @mechanism = mechanism.to_sym
      @options = options

      unless [:PLAIN, :GSSAPI].include? @mechanism
        raise "Unknown SASL mechanism: #{@mechanism}"
      end

      if @mechanism == :GSSAPI
        @gsscli = GSSAPI::Simple.new(@options[:host], @options[:principal])
      end
    end

    def open
      super

      case @mechanism
      when :PLAIN
        handshake_plain!
      when :GSSAPI
        handshake_gssapi!
      end
    end

    private

    def handshake_plain!
      username = @options.fetch(:username, 'anonymous')
      password = @options.fetch(:password, 'anonymous')

      token = "[PLAIN]\u0000#{username}\u0000#{password}"
      write_handshake_message(NEGOTIATION_STATUS[:START], 'PLAIN')
      write_handshake_message(NEGOTIATION_STATUS[:OK], token)

      status, _ = read_handshake_message
      case status
      when NEGOTIATION_STATUS[:COMPLETE]
        @open = true
      when NEGOTIATION_STATUS[:OK]
        raise "Failed to complete challenge exchange: only NONE supported currently"
      end
    end

    def handshake_gssapi!
      token = @gsscli.init_context
      write_handshake_message(NEGOTIATION_STATUS[:START], 'GSSAPI')
      write_handshake_message(NEGOTIATION_STATUS[:OK], token)

      status, msg = read_handshake_message
      case status
      when NEGOTIATION_STATUS[:COMPLETE]
        raise "Unexpected COMPLETE from server"
      when NEGOTIATION_STATUS[:OK]
        unless @gsscli.init_context(msg)
          raise "GSSAPI: challenge provided by server could not be verified"
        end

        write_handshake_message(NEGOTIATION_STATUS[:OK], "")

        status, msg = read_handshake_message
        case status
        when NEGOTIATION_STATUS[:COMPLETE]
          raise "Unexpected COMPLETE from server"
        when NEGOTIATION_STATUS[:OK]
          unwrapped = @gsscli.unwrap_message(msg)
          rewrapped = @gsscli.wrap_message(unwrapped)

          write_handshake_message(NEGOTIATION_STATUS[:COMPLETE], rewrapped)

          status, msg = read_handshake_message
          case status
          when NEGOTIATION_STATUS[:COMPLETE]
            @open = true
          when NEGOTIATION_STATUS[:OK]
            raise "Failed to complete GSS challenge exchange"
          end
        end
      end
    end

    def read_handshake_message
      status, len = @transport.read(STATUS_BYTES + PAYLOAD_LENGTH_BYTES).unpack('cl>')
      body = @transport.to_io.read(len)
      if [NEGOTIATION_STATUS[:BAD], NEGOTIATION_STATUS[:ERROR]].include?(status)
        raise "Exception from server: #{body}"
      end

      [status, body]
    end

    def write_handshake_message(status, message)
      header = [status, message.length].pack('cl>')
      @transport.write(header + message)
    end
  end

  class SASLTransportFactory < Thrift::BaseTransportFactory
    def get_transport(transport)
      return SASLTransport.new(transport)
    end
  end
end
