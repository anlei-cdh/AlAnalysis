package com.al.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TimePartition extends Partitioner<TimeWritable, IntWritable> {

	@Override
	public int getPartition(TimeWritable key, IntWritable value, int numPartitions) {
		return (key.getUuid().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
