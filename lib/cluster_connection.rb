
require 'rubygems'
require 'mongo'

module Mongo
  class NoSharding < Exception; end

  class Connection
    def dbgrid?
      begin
        self['admin'].command( { :isdbgrid => 1} )
      rescue Mongo::OperationFailure
        return false
      end
    end

    def shards
      return nil if !dbgrid?

      @shards ||= self['admin'].command( { :listShards => 1} )["shards"]
    end

    def stop_balancer
      raise Mongo::NoSharding   if not dbgrid?

      self['config']['settings'].update( { :_id => "balancer" }, { :stopped => true } )
      if block_given?
        begin
          yield
        ensure
          start_balancer
        end
      end
    end

    def start_balancer
      raise Mongo::NoSharding   if not dbgrid?

      self['config']['settings'].update( { :_id => "balancer" }, { :stopped => false } )
    end

    def lock_node(&block)
      lock!
      begin
        yield
      ensure
        unlock!
      end
    end
  end

  
  class ReplSetConnection
    def passives
      @passives ||= self['local']['system.replset'].find_one()['members'].reject do |member|
        member["priority"] != 0
      end.map{ |m| m["host"] }
    end

    def name
      @replica_set
    end
  end

end
