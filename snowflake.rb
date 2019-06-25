#!/usr/bin/ruby -w
#encoding: utf-8

require "time"

module SnowFlake
  class Worker
    attr_accessor :workerID, :datacenterID, :lock, :timestamp, :sequence

	#EPOCH 时间偏移量，从2019年6月16日零点开始
	EPOCH = Time.local(2019, 6, 16, 0, 0, 0, 0)
	#SEQUENCE_BITS 自增量占用比特
	SEQUENCE_BITS = 12
	#WORKERID_BITS 工作进程ID比特
	WORKERID_BITS = 5
	#DATACENTERID_BITS 数据中心ID比特
	DATACENTERID_BITS = 5
	#NODEID_BITS 节点ID比特
	NODEID_BITS = DATACENTERID_BITS + WORKERID_BITS
	#SEQUENCE_MASK 自增量掩码（最大值）
	SEQUENCE_MASK = -1 ^ (-1 << SEQUENCE_BITS)
	#DATACENTERID_LEFT_SHIFT_BITS 数据中心ID左移比特数（位数）
	DATACENTERID_LEFT_SHIFT_BITS = WORKERID_BITS + SEQUENCE_BITS
	#WORKERID_LEFT_SHIFT_BITS 工作进程ID左移比特数（位数）
	WORKERID_LEFT_SHIFT_BITS = SEQUENCE_BITS
	#NODEID_LEFT_SHIFT_BITS 节点ID左移比特数（位数）
	NODEID_LEFT_SHIFT_BITS = DATACENTERID_BITS + WORKERID_BITS + SEQUENCE_BITS
	#TIMESTAMP_LEFT_SHIFT_BITS 时间戳左移比特数（位数）
	TIMESTAMP_LEFT_SHIFT_BITS = NODEID_LEFT_SHIFT_BITS
	#WORKERID_MAX 工作进程ID最大值
	WORKERID_MAX = -1 ^ (-1 << WORKERID_BITS)
	#DATACENTERID_MAX 数据中心ID最大值
	DATACENTERID_MAX = -1 ^ (-1 << DATACENTERID_BITS)
	#NODEID_MAX 节点ID最大值
	NODEID_MAX = -1 ^ (-1 << NODEID_BITS)

    def initialize(datacenterID = 0, workerID = 0)
      @timestamp = 0
      @datacenterID = datacenterID
      @workerID = workerID
      @sequence = 0
      @lock = Monitor.new
      valid
    end
    def generate
      @lock.synchronize do
        now = self.epoch
        if now == @timestamp
          @sequence = (@sequence + 1) & SEQUENCE_MASK
          if @sequence == 0
            self.wait now
          end
        else
          @sequence = rand(10)
        end
        @timestamp = now
        self.id
      end
    end
    # wait next millisecond
    protected
    def wait(now)
      while (now <= @timestamp)
        now = self.epoch
      end
      @timestamp = now
    end
    # generator snowflake id
    protected
    def id
      (@timestamp << TIMESTAMP_LEFT_SHIFT_BITS) |
        (@datacenterID << DATACENTERID_LEFT_SHIFT_BITS) |
        (@workerID << WORKERID_LEFT_SHIFT_BITS) |
        @sequence
    end
    # epoch millisecond
    protected
    def epoch
      Time.now.strftime("%s%L").to_i - (EPOCH.to_i * 1000)
    end
    # valid workerID and datacenterID range
    private
    def valid
      if @workerID < 0 || @workerID > WORKERID_MAX
        raise "Invalid workerID"
      end
      if @datacenterID < 0 || @datacenterID > DATACENTERID_MAX
        raise "Invalid datacenterID"
      end
    end
  end

  class Node < Worker
    def initialize(nodeID = 0)
      @nodeID = nodeID
      @datacenterID = @nodeID >> DATACENTERID_BITS
      @workerID = @nodeID & (-1 ^ (-1 << WORKERID_BITS))
      valid
      @worker = Worker.new(@datacenterID, @workerID)
    end
    def generate
      @worker.generate
    end
    # valid nodeID range
    private
    def valid
      if @nodeID < 0 || @nodeID > NODEID_MAX
        raise "Invalid nodeID"
      end
    end
  end

  class ID
    def initialize(id = 0)
      @id = id
    end
    # Parse for the SnowFlake ID
    def parse()
      r = Hash.new
      r[:timestamp] = @id >> Worker::TIMESTAMP_LEFT_SHIFT_BITS
      r[:time] = Worker::EPOCH.to_i + (@id >> Worker::TIMESTAMP_LEFT_SHIFT_BITS) / 1000.0
      r[:node] = (@id >> Worker::WORKERID_LEFT_SHIFT_BITS) & (-1 ^ (-1 << Worker::NODEID_BITS))
      r[:sequence] = @id & Worker::SEQUENCE_MASK
      r
    end
  end
end
