package com.al.util;

import com.al.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class HadoopUtil {
	
	/**
	 * 创建新文件，并写入
	 */
	public static void createFile(Configuration conf, String file, List<?> list) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(file);
        if(!fs.exists(path)) {
	        FSDataOutputStream os = fs.create(path);
	        for(Object entity : list) {
	        	String line = entity.toString() + Config.newline;
	        	os.write(line.getBytes("UTF-8"));
	        }
	        os.close();
	        System.out.println(file + " write hdfs!");
        } else {
        	System.out.println(file + " already exists!");
        }
        fs.close();
    }
	
	/**
	 * 创建文件夹
	 */
	public static void createDir(Configuration conf, String dir) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		fs.mkdirs(new Path(dir));
		fs.close();
    }

	/**
	 * 删除目录
	 */
	public static void deleteDir(Configuration conf, String dir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(dir), true);
        fs.close();
    }
	
	/**
	 * 本地Hadoop
	 */
	public static void setLocalHadoop(Configuration conf)  {
		conf.set("mapred.job.tracker", "local");
		conf.set("mapreduce.framework.name", "null");
		conf.set("dfs.permissions", "true");
		conf.set("dfs.name.dir", "file:///tmp/hadoop-Administrator/dfs/name");
		conf.set("fs.default.name", "file:///");
    }
	
	/**
	 * 检查文件
	 */
	public static boolean existsFile(Configuration conf, String file) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(file);
        return fs.exists(path);
    }
	
	/**
	 * 查看文件大小
	 */
	public static long getFileSize(Configuration conf, String file) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(file);
        FileStatus status = fs.getFileStatus(path);
        return status.getLen();
    }

	/**
	 * 根据is_local来判断 删除服务器或者本地的输出文件夹
	 */
	public static void delServerOrLocalFolder(Configuration conf, String output_path) throws Exception {
		if(Config.is_local) {
			setLocalHadoop(conf);
			FileUtil.deleteDirectory(output_path);
		} else {
			deleteDir(conf, output_path);
		}
	}

	/**
	 * 获得MapReduce的结果 并返回集合
	 * @param reduce_result_path
	 * @return
	 * @throws IOException
	 */
	public static List<BufferedReader> getReduceResultList(String reduce_result_path) throws IOException  {
		List<BufferedReader> list = new ArrayList<BufferedReader>();
		InputStreamReader reader = null;
		if(Config.is_local) {
			reduce_result_path += Config.backslash + Config.reduce_result_filename;
			reader = new FileReader(reduce_result_path);
			BufferedReader br = new BufferedReader(reader);
			list.add(br);
		} else {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path paths = new Path(reduce_result_path);
			FileStatus fileStatus [] = fs.listStatus(paths);

			for(FileStatus status : fileStatus) {
				Path path = new Path(status.getPath().toString());
				FSDataInputStream is = fs.open(path);
				BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
				list.add(br);
			}
		}
		return list;
	}
	
	public static void main(String[] args) {
		
	}

}
