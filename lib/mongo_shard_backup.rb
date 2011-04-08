$LOAD_PATH.push File.join(File.dirname(__FILE__),"..","aws_utils")

require 'rubygems'
require 'cluster_connection'
require 'socket'
require 'right_aws'
require 'logger'
require 'ec2'

class MongoShardBackup

  attr_reader :backed_volumes

  def initialize(cluster, opts={})
    raise Mongo::NoSharding   if not cluster.dbgrid?

    # TODO: add description
    @opts={
      :device_name => '/dev/sda1'
    }.merge(opts)
    
    @backed_volumes=[]
    @cluster = cluster
  end

  def backup
    @cluster.stop_balancer

    begin
      @cluster.shards.each do |shard|
        replica = connect_to_replica( shard["host"] )
        passives = connect_to_passives( replica.passives )
        passives.each { |node|
          node.lock!
          begin
            snapshot_node(node.host)
          rescue => e
          ensure
            node.unlock!
            raise e if e
          end
        }
      end

      snapshot_node(@cluster.host)   # snapshot the config server
                                    # one of them should be on the same
                                    # node as mongos router
    rescue => e
    ensure
      @cluster.start_balancer
      raise e if e
    end
    @backed_volumes
  end

  def snapshot_node(node)
    instance=EC2Conn.find_instance_by_ip(Socket.getaddrinfo(node, 27017)[0][3])
    instance[:block_device_mappings].each{|ebs|
      if ebs[:device_name]==@opts[:device_name]
        @backed_volumes.push ebs[:ebs_volume_id]  # TODO: code the process
      end
    }
  end

  def connect_to_replica(replica)
    Mongo::ReplSetConnection.new( *split_names(replica) )
  end

  def connect_to_passives(passives)
    passives.map {|host|
      node = split_names(host).flatten.compact
      Mongo::Connection.new(*node)
    }
  end

  #   splits replica string into array to feed it into Mongo::ReplSetConnection and
  #   into Mongo::Connection

  def split_names(replica)
    names = replica.match /(([^\/]*)\/)?(([^:,]*)(:(\d+))?)(,([^:,]*)(:(\d+))?)?(,([^:,]*)(:(\d+))?)?/
    [
      [ names[4], names[6].nil? ? nil : names[6].to_i ],
      [ names[8], names[10].nil? ? nil : names[10].to_i ],
      [ names[12], names[14].nil? ? nil : names[14].to_i ],
      { :rs_name => names[2] }
    ].reject{ |x| x.empty? }
  end


#  allow usage of MongoBackup.backup(m) to make backups
  def self.backup(*args)
    cluster=new(*args)
    cluster.backup
  end
end
