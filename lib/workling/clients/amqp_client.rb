require 'workling/clients/base'
Workling.try_load_an_amqp_client

#
#  An Ampq client
#
module Workling
  module Clients
    class AmqpClient < Workling::Clients::Base
      
      # starts the client. 
      def connect
        begin
          options = Workling.config[:amqp_options]
          
          options[:host]  ||= '127.0.0.1'
          options[:port]  ||= 5672
          options[:vhost] ||= '/'
          options[:user]  ||= 'guest'
          options[:pass]  ||= 'guest'
          options[:timeout] = options[:timeout] ? (options[:timeout] =~ /^false$/i ? nil : options[:timeout].to_i) : nil
          options[:logging] = ((options[:logging] || 'false') =~ /^true$/i) == 0

          connection = AMQP.connect(options)
          @amq = MQ.new(connection)
        rescue
          raise WorklingError.new("couldn't start amq client. if you're running this in a server environment, then make sure the server is evented (ie use thin or evented mongrel, not normal mongrel.); #{$!.to_s}")
        end
      end
      
      # no need for explicit closing. when the event loop
      # terminates, the connection is closed anyway. 
      def close; true; end
      
      # subscribe to a queue
      def subscribe(key)
        @amq.queue(key.to_s).subscribe do |value|
          yield Marshal.load(value)
        end
      end
      
      # request work
      def request(key, value); @amq.queue(key.to_s).publish(Marshal.dump(value)); end
    end
  end
end