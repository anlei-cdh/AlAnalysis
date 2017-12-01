package com.al.run;

import com.al.hive.HiveAnalysis;
import com.al.mapreduce.TimeRun;

public class HadoopAnalysis {

    public static void main(String[] args) {
        HiveAnalysis.runAnalysis();
        TimeRun.runAnalysis();
    }

}
