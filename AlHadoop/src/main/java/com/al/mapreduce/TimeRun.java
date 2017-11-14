package com.al.mapreduce;

public class TimeRun {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TimeRun.runAnalysis();
	}

	public static void runAnalysis() {
		try {
			TimeDriver dirver = new TimeDriver();
			dirver.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
