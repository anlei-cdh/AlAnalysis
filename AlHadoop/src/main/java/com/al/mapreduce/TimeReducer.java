package com.al.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class TimeReducer extends Reducer<TimeWritable, IntWritable, TimeWritable, NullWritable> {
	private long all_time = 0L;
	@Override
	public void reduce(TimeWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		long time = 0L;
		long ts = 0L;
		while (values.iterator().hasNext()) {
			values.iterator().next();
			if(ts != 0L) {
				time += key.getTs() - ts;
			}
			ts = key.getTs();
		}
		time += 10L;
		all_time += time;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		TimeWritable timeWritable = new TimeWritable();
		timeWritable.setTime(all_time);
		context.write(timeWritable, NullWritable.get());
	}
}
