package com.q.sparkapplicationdemo;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

public class sparkApplicationDemo {
    /**
     * @param args
     * spark session - enable Hive
     */
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("sparkApplicationDemo")
                .master("local[*]")
//                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();

        System.out.println("spark application started" + spark);
        System.out.println("spark application started" + spark.version());

        String csvDirectoryPath = "/Users/q_dev/Downloads";

        dataProcessWithRecovery(spark, csvDirectoryPath);

        spark.stop();
    }
    /**
     * CSV data 확인
     */
    public static void dataProcessWithRecovery(SparkSession spark, String csvDirectoryPath) {
        try {
            // CSV 파일
            StructType schema = new StructType(new StructField[]{
                    new StructField("event_time", DataTypes.TimestampType,true, Metadata.empty())
            });

            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .schema(schema)
                    .csv(csvDirectoryPath + "/2019-Oct.csv")
                    .limit(10);

            df.show();

        } catch (Exception e) {
            // Log the error and handle recovery
            System.err.println("Batch processing failed: " + e.getMessage());
        }
    }
}