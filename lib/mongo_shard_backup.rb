require 'rubygems'
require 'cluster_connection'
require 'socket'
require 'logger'
require 'date'
require 'timurb-ec2'

class MongoShardBackup
  CREATED_BY = 'Created by MongoShardBackup'

  attr_reader :backed_volumes, :created_snapshots


  def initialize(cluster, opts={})
    @opts={
      :device_name => '/dev/sda1',
      :description => 'Backup of $c($i,$v), $d [$n]',
      :env => cluster.to_s,
      :tags => {},
      :logfile => STDERR,
    }.merge(opts)

    setup_logger

    if cluster.is_a?(String)
      @logger.info("Connecting to #{cluster}")
      cluster = Mongo::Connection.new(cluster, nil, :logger=>@logger )
    end

    raise Mongo::NoSharding   if not cluster.dbgrid?

    @backed_volumes = []
    @created_snapshots = []
    @cluster = cluster
  end

  def run
    @backup_id = @opts[:env]+Time.now.to_i.to_s
    @locked_nodes = []
    @cluster.stop_balancer do
      begin
        @cluster.shards.each do |shard|
          replica = connect_to_replica( shard["host"] )
          passives = connect_to_passives( replica.passives )
          passives.each { |node|
            snapshot_with_lock(node, replica.name)
          }
        end

        # config server to snapshot should be running
        # on the same host as mongos router at port 38019
        config = connect_to_config( @cluster.host )

        #   it seems locking the config server is unsafe, so snapshot it without locking
        snap = snapshot_node(config.host)
        tag_snapshot(snap, 'CONFIG', config.host)

        wait_snapshots

      ensure
        @locked_nodes.compact!

        @locked_nodes.each do |node|
          host = node.host          # we need to save that before checking for connection
          err = node.safe_unlock!          
          @logger.error("Node #{host} NOT unlocked! : #{err}")    if err
        end
      end
    end

    return @created_snapshots
  end

  def snapshot_with_lock(node, replica)
    locked = node.lock_node!
    if locked
      @locked_nodes.push( node )
      snap = snapshot_node(node.host)
      tag_snapshot(snap, replica, node.host)
    else
      @logger.warning("Could not lock node #{node.host}. No snapshotting")
    end    
  end

  def snapshot_node( node )
    @logger.debug("Searching for instance by ip #{node}")
    instance = EC2Conn.find_instance_by_ip(Socket.getaddrinfo(node, 27017)[0][3])

    @logger.debug("Searching for volume by instance id #{instance[:aws_instance_id]}")
    vol = EC2Conn.instance_vol_by_name( instance, @opts[:device_name] )

    @logger.info("Snapshotting the #{vol} (#{@opts[:device_name]}) of #{instance[:aws_instance_id]} (#{node})")
    snapshot = EC2Conn.create_snapshot( vol, description(instance, vol) )

    @backed_volumes.push( vol )
    @created_snapshots.push( snapshot[:aws_id] )
    return snapshot
  end

  def wait_snapshots
    @logger.info("Waiting for snapshots #{@created_snapshots.join(", ")} to finish")
    EC2Conn.wait_snapshot( @created_snapshots ) unless @opts[:nowait]
  end

  def tag_snapshot( snap, replica, node)
    @logger.debug("Adding tags to snapshot #{snap[:aws_id]}")
    EC2Conn.create_tags(snap[:aws_id],
      { "BackupEnvironment" => @opts[:env],
        "BackupReplica" => replica,
        "BackupNode" => node,            # do we need that?
        "BackupVol"  => snap[:aws_volume_id],
        "BackupDate" => Date.today.to_s,
        "BackupID" => @backup_id,
      }.merge(@opts[:tags]) )
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
    Mongo::Connection.new(host,38019, :logger => @logger)
  end

  #   splits replica string into array to feed it into Mongo::ReplSetConnection and
  #   into Mongo::Connection

  def split_names(replica)
    names = replica.match /(([^\/]*)\/)?(([^:,]*)(:(\d+))?)(,([^:,]*)(:(\d+))?)?(,([^:,]*)(:(\d+))?)?/
    [
      [ names[4], names[6].nil? ? nil : names[6].to_i ],
      [ names[8], names[10].nil? ? nil : names[10].to_i ],
      [ names[12], names[14].nil? ? nil : names[14].to_i ],
      { :rs_name => names[2],
        :logger => @logger,
      }
    ].reject{ |x| x.empty? }
  end

  def setup_logger
    @logger = Logger.new( @opts[:logfile] )
    @logger.level = @opts[:verbose] || 3
    EC2Conn.ec2( :logger => @logger )
  end

#  you can use MongoBackup.backup(m) to make backups
  def self.backup(*args)
    backup=self.new(*args)
    backup.run
  end
end
