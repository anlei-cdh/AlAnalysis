package com.al.mapreduce;

import com.al.config.Config;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TimeComparator extends WritableComparator {

	protected TimeComparator() {
		super(TimeWritable.class, true);
	}
	
	@Override 
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		TimeWritable o1 = (TimeWritable)a;
		TimeWritable o2 = (TimeWritable)b;

		if(o1.getUuid().equals(o2.getUuid())) {
			long time = o2.getTime() - o1.getTime();
			return  time < Config.default_ses_time ? 0 : 1;
		} else {
			return 1;
		}
	} 

}
