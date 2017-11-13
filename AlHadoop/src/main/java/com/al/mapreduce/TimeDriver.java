package com.al.mapreduce;

import com.al.config.Config;
import com.al.dao.DimensionDao;
import com.al.db.DBHelper;
import com.al.entity.Dimension;
import com.al.util.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.sql.Connection;
import java.util.List;


public class TimeDriver {
	
	public void run() throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.queue.name","al");
		
		String input_path = Config.input_path;
		String output_path = Config.output_path;

		HadoopUtil.delServerOrLocalFolder(conf, output_path);

		Job job = Job.getInstance(conf, "Time");
		job.setJarByClass(TimeDriver.class);
		
		job.setMapperClass(TimeMapper.class);
		job.setReducerClass(TimeReducer.class);
		
		job.setGroupingComparatorClass(TimeComparator.class);
		job.setPartitionerClass(TimePartition.class);
		
		job.setMapOutputKeyClass(TimeWritable.class);
	    job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(TimeWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));
		
		job.waitForCompletion(true);

		getResultAndSaveData(output_path);
	}

	public void getResultAndSaveData(String output_path) throws Exception {
		List<BufferedReader> list = HadoopUtil.getReduceResultList(output_path);
		if(list != null) {
			String line = null;
			for(BufferedReader br : list) {
				while ((line = br.readLine()) != null) {
					long time = Long.parseLong(line.trim());
					Dimension dimension = new Dimension();
					dimension.setTime(time);
					dimension.setDay(Config.day);
					saveMapreduceDimensionData(dimension);
				}
			}
		}
	}

	public void saveMapreduceDimensionData(Dimension dimension) {
		Connection conn = DBHelper.getConnection();
		try {
			DimensionDao.saveMapreduceDimensionData(dimension, conn);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBHelper.close(conn);
		}
	}
	
	public static void main(String [] args) {
		
	}
}
