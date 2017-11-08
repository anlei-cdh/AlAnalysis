package com.al.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimeWritable implements WritableComparable<TimeWritable> {

	private long ts;
	private String uuid;
	private long time;

	public long getTs() {
		return ts;
	}
	public void setTs(long ts) {
		this.ts = ts;
	}
	public String getUuid() {
		return uuid;
	}
	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		this.uuid = input.readUTF();
		this.ts = input.readLong();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(uuid);
		output.writeLong(ts);
	}

	@Override
	public int compareTo(TimeWritable that) {
		int compare = this.uuid.compareTo(that.uuid);
		if(compare == 0) {
			return Long.compare(this.ts, that.ts);
		}
		return compare;
	}
	
	@Override
	public String toString() {
		return String.valueOf(time);
	}

}
