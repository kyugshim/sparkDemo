package com.q.sparkapplicationdemo;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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
                .config("hive.metastore.uris", "thrift://localhost:9083")
                .enableHiveSupport()
                .getOrCreate();

        System.out.println("spark application started" + spark);

        spark.sql("SHOW DATABASES").show();


        String csvDirectoryPath = "/Users/q_dev/Downloads";
        String checkPointPath = "/Users/q_dev/project/sparkDemo/checkpoints";
        String outputPath = "/Users/q_dev/Downloads/sparkDemo/parquet";

        dataProcessWithRecovery(spark, csvDirectoryPath, checkPointPath, outputPath);

        spark.stop();
    }
    /**
     * CSV data 확인
     */
    public static void dataProcessWithRecovery(SparkSession spark, String csvDirectoryPath, String checkPointPath, String outputPath) {
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

            Set<String> processedFiles = getProcessedFiles(checkPointPath);

            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .schema(schema)
                    .csv(csvDirectoryPath + "/2019-Oct.csv")
                    .limit(1);

            // KST 기준으로 daily 추가
            df = df.withColumn("event_date", functions.from_utc_timestamp(df.col("event_time"), "Asia/Seoul").cast("date"));

            // KST 기준으로 daily partition 으로 저장 (Parquet with Snappy compression)
            df.write()
                    .partitionBy("event_date")
                    .mode(SaveMode.Append)
                    .parquet(outputPath);

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
                    "LOCATION '" + outputPath + "'");

//            System.out.println("input file : " + Arrays.toString(df.inputFiles()));

            checkPointSuccessfulBatch(checkPointPath, df.inputFiles());

            df.show();

        } catch (Exception e) {
            // Log the error and handle recovery
            System.err.println("Batch processing failed: " + e.getMessage());
        }
    }

    /**
     *
     * @param checkPointPath
     * @param processedFiles
     * @throws IOException
     */
    public static void checkPointSuccessfulBatch(String checkPointPath, String[] processedFiles) throws IOException{
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "file:///");
        FileSystem fs = FileSystem.get(conf);
        Path checkpointFile = new Path(checkPointPath + "/last_batch");

        if (fs.exists(checkpointFile)) {
            fs.delete(checkpointFile,true);
        }
        FSDataOutputStream out = fs.create(checkpointFile);
        for (String file : processedFiles) {
            out.writeUTF(file);
        }
        out.close();
        System.out.println("Checkpoint successful");
    }
    private static Set<String> getProcessedFiles(String checkPointPath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "file:///");
        FileSystem fs = FileSystem.get(conf);
        Path checkpointFile = new Path(checkPointPath + "/last_batch");
        Set<String> processedFiles = new HashSet<>();
        if (fs.exists(checkpointFile)) {
            FSDataInputStream in = fs.open(checkpointFile);
            while (in.available() > 0) {
                processedFiles.add(in.readUTF());
            }
            in.close();
        }
        return processedFiles;
    }
}