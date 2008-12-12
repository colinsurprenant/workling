require "workling/remote/runners/not_remote_runner"
require "workling/remote/runners/spawn_runner"
require "workling/remote/runners/starling_runner"
require "workling/remote/runners/backgroundjob_runner"

require 'digest/md5'

#
#  Scoping Module for Runners. 
#
module Workling
  module Remote
    
    # set the desired runners here. this is initialized with Workling.default_runner. 
    # dispatcher can be used as a shortcut instead of specifying submitter & requester
    # since most apps will use the same runner on both the app end and the workers end.
    mattr_accessor :dispatcher
    
    # submitter and requester are both ends of the dispatcher and can be set individually
    # if the app end needs to use a different runner than on the worker end.
    mattr_accessor :submitter
    mattr_accessor :requester

    # set the desired invoker. this class grabs work from the job broker and executes it. 
    mattr_accessor :invoker
    @@invoker ||= Workling::Remote::Invokers::ThreadedPoller
    
    # retrieve the dispatcher or instantiate it using the defaults
    def self.dispatcher
      @@dispatcher ||= Workling.default_runner
    end
    
    # retrieve the submitter or grab from do-all dispatcher
    def self.submitter
      @@submitter ||= self.dispatcher
    end

    # retrieve the requester or or grab from do-all dispatcher
    def self.requester
      @@requester ||= self.dispatcher
    end

    # generates a unique identifier for this particular job. 
    def self.generate_uid(clazz, method)
      uid = ::Digest::MD5.hexdigest("#{ clazz }:#{ method }:#{ rand(1 << 64) }:#{ Time.now }")
      "#{ clazz.to_s.tableize }/#{ method }/#{ uid }".split("/").join(":")
    end
    
    # dispatches to a workling. writes the :uid for this work into the options hash, so make 
    # sure you pass in a hash if you want write to a return store in your workling.
    def self.run(clazz, method, options = {})
      uid = Workling::Remote.generate_uid(clazz, method)
      options[:uid] = uid if options.kind_of?(Hash) && !options[:uid]
      Workling.find(clazz, method) # this line raises a WorklingError if the method does not exist. 
      self.submitter.run(clazz, method, options)
      uid
    end
  end
end
