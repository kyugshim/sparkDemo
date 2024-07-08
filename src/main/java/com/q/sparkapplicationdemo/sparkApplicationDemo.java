package com.q.sparkapplicationdemo;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


public class sparkApplicationDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("sparkApplicationDemo")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        System.out.println("spark application started" + spark);
        System.out.println("spark application started" + spark.version());

        spark.stop();
    }
}