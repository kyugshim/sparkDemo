package com.q.sparkapplicationdemo;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


public final class sparkApplicationDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkPi")
                .master("local[*]")
                .getOrCreate();

        System.out.println();

        spark.stop();
    }
}