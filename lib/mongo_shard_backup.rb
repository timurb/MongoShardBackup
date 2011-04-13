require 'rubygems'
require 'cluster_connection'
require 'socket'
require 'logger'
require 'date'
require 'timurb-ec2'

class MongoShardBackup
  CREATED_BY = 'Created by MongoShardBackup'

  attr_reader :backed_volumes


  def initialize(cluster, opts={})
    @opts={
      :device_name => '/dev/sda1',
      :description => 'Backup of $c($i,$v), $d [$n]',
    }.merge(opts)

    cluster = Mongo::Connection.new(cluster) if cluster.is_a?(String)

    raise Mongo::NoSharding   if not cluster.dbgrid?

    @backed_volumes = []
    @created_snapshots = []
    @cluster = cluster
  end

  def backup
    @cluster.stop_balancer do
      @cluster.shards.each do |shard|
        replica = connect_to_replica( shard["host"] )
        passives = connect_to_passives( replica.passives )
        passives.each { |node|
          node.lock_node do
            snapshot_node(node.host)
          end
        }
      end

      # config server to snapshot should be running 
      # on the same host as mongos router at port 27019      
      config = connect_to_config( @cluster.host )
      config.lock_node do
        snapshot_node(config.host)
      end
    end

    return @created_snapshots
  end

  def snapshot_node( node )
    instance = EC2Conn.find_instance_by_ip(Socket.getaddrinfo(node, 27017)[0][3])

    vol = EC2Conn.instance_vol_by_name( instance, @opts[:device_name] )
    snapshot = EC2Conn.create_snapshot( vol, description(instance, vol) )

    EC2Conn.wait_snapshot( snapshot[:aws_id] ) unless @opts[:nowait]

    @backed_volumes.push( vol )
    @created_snapshots.push( snapshot[:aws_id] )
    return snapshot
  end

  def description(instance, volume)
    instance = instance[:aws_instance_id] unless instance.is_a?(String)
    desc = @opts[:description]
    desc.gsub!('$c', @cluster.host.to_s )
    desc.gsub!('$i', instance )
    desc.gsub!('$v', volume )
    desc.gsub!('$d', Date.today.to_s )
    desc.gsub!('$n', CREATED_BY )
    return desc
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

  def connect_to_config(host)
    Mongo::Connection.new(host,27019)
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


#  you can use MongoBackup.backup(m) to make backups
  def self.backup(*args)
    cluster=new(*args)
    cluster.backup
  end
end
