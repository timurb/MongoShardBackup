# To change this template, choose Tools | Templates
# and open the template in the editor.

require 'mongo'

class MongoServer

  attr_reader :conn, :admin, :config

  def initialize(host=nil,port=nil)
    @conn=Mongo::Connection.new(host,port)
    @admin=@conn['admin']
    @config=@conn['config']
  end

  def lock

  end

  def unlock

  end

end
