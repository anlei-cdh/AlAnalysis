package com.al.util;

import com.al.hive.HiveAnalysis;
import com.al.mapreduce.TimeRun;

public class AllAnalysis {

    public static void main(String[] args) {
        HiveAnalysis.runAnalysis();
        TimeRun.runAnalysis();
    }

}
