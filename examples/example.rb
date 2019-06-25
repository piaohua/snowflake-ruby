#!/usr/bin/ruby -w
#encoding: utf-8

require_relative "../snowflake"

sf = SnowFlake::Worker.new(1, 1)
puts SnowFlake::Worker::EPOCH
#puts sf.generate
puts "workerID: #{sf.workerID}, datacenterID: #{sf.datacenterID}"

n = 10
while (n > 1)
  puts "id: #{sf.generate}, epoch: #{sf.timestamp}, sequence: #{sf.sequence}"
  n = n - 1
end

node = SnowFlake::Node.new(461)
puts "workerID: #{node.workerID}, datacenterID: #{node.datacenterID}"
puts node.generate

id = node.generate
r = SnowFlake::ID.new(id).parse
puts r
puts Time.at(r[:time])
