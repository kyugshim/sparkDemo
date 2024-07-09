package com.q.sparkapplicationdemo;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
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
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
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
            // CSV 파일 data schema
            StructType schema = new StructType(new StructField[]{
                    new StructField("event_time", DataTypes.TimestampType,true, Metadata.empty()),
                    new StructField("event_type", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("product_id", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("category_id", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("category_code", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("brand", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("price", DataTypes.DoubleType, true, Metadata.empty()),
                    new StructField("user_id", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("user_session", DataTypes.StringType, true, Metadata.empty()),
            });

            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .schema(schema)
                    .csv(csvDirectoryPath + "/2019-Oct.csv")
                    .limit(5000);

            // KST 기준으로 daily 추가
            df = df.withColumn("event_date", functions.from_utc_timestamp(df.col("event_time"), "Asia/Seoul").cast("date"));

            // KST 기준으로 daily partition 으로 저장 (Parquet with Snappy compression)
            df.write()
                    .partitionBy("event_date")
                    .mode(SaveMode.Append)
                    .parquet("/Users/q_dev/project/sparkDemo/parquet");

            // external Hive table 필요시 생성
            spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS userActivityLog (" +
                    "event_time TIMESTAMP, " +
                    "event_type STRING, " +
                    "product_id STRING, " +
                    "category_id STRING, " +
                    "category_code STRING, " +
                    "brand STRING, " +
                    "price DOUBLE, " +
                    "user_id STRING, " +
                    "user_session STRING" +
                    ") PARTITIONED BY (event_date DATE) " +
                    "STORED AS PARQUET " +
                    "LOCATION '/Users/q_dev/project/sparkDemo/parquet'");

            df.show();

        } catch (Exception e) {
            // Log the error and handle recovery
            System.err.println("Batch processing failed: " + e.getMessage());
        }
    }
}