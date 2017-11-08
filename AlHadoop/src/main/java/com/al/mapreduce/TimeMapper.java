package com.al.mapreduce;

import com.alibaba.fastjson.JSON;
import com.al.entity.Log;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class TimeMapper extends Mapper<Object, Text, TimeWritable, IntWritable>  {
	private final IntWritable one = new IntWritable(1);
	TimeWritable dimeWritable = new TimeWritable();
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		Log log = JSON.parseObject(line, Log.class);
		if(!log.isLegal()) {
			return ;
		}

		dimeWritable.setUuid(log.getUuid());
		dimeWritable.setTs(log.getTs());
		context.write(dimeWritable, one);
	}
}
