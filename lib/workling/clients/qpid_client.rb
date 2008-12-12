require 'workling/clients/base'
Workling.try_load_qpid_client

#
#  A Qpid client
#
module Workling
  module Clients
    class QpidClient < Workling::Clients::Base
      
      EMPTY_HEADER = {}
            
      # starts the client. 
      def connect
        begin
          options = Workling.config[:qpid_options]
 
          qpid_gemspec = Gem.loaded_specs['colinsurprenant-qpid'] || Gem.loaded_specs['qpid']
          spec  = options[:spec]  || "#{qpid_gemspec.full_gem_path}/specs/official-amqp0-8.xml"
          host  = options[:host]  || '127.0.0.1'
          port  = options[:port]  || 5672
          vhost = options[:vhost] || '/'
          user  = options[:user]  || 'guest'
          pass  = options[:pass]  || 'guest'
          
          @client = Qpid::Client.new(host, port.to_i, Qpid::Spec::Loader.build(spec), vhost)
          @client.start({ "LOGIN" => user, "PASSWORD" => pass })
          @channel = @client.channel(1)
          @channel.channel_open
        rescue
          raise WorklingError.new("couldn't start Qpid; #{$!.to_s}")
        end
        return true
      end
      
      def close
        @channel.channel_close
        @client.close
      end
              
      def request(key, value)
        c = Qpid::Content.new(EMPTY_HEADER, Marshal.dump(value))
        @channel.basic_publish(:routing_key => key.to_s, :content => c)
      end
      
      # TODO: retrieve has not been tested
      def retrieve(key)
        unless @queue
          @channel.queue_declare(:queue => key)
          @channel.queue_bind(:queue_name => key)
          bc = channel.basic_consume(:queue => key)
          @queue = client.queue(bc.consumer_tag)
        end
        
        return nil if @queue.empty?
        begin
          # at this point there should be something in the queue but nonetheless we keep a
          # defensive approach and trap a possible empty queue exception. there could be a 
          # race condition between the empty? check and the pop.
          message = @queue.pop(non_block = true)
          return Marshal.load(message.content.body)
        rescue
          return nil
        end
      end

    end
  end
end