# To change this template, choose Tools | Templates
# and open the template in the editor.

require 'rubygems'
require 'mongo'

class MongoServer

  attr_reader :conn, :admin, :config

  def initialize(host=nil,port=nil)
    @conn=Mongo::Connection.new(host,port)
    @admin=@conn['admin']
    @config=@conn['config']
  end

  def lock
    @admin.command( {"fsync"=>1} )
  end

  def unlock
  end

end
