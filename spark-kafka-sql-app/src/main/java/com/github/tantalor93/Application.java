package com.github.tantalor93;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

public class Application {

    public static void main(String[] args) throws StreamingQueryException {
        final SparkSession sparkSession = SparkSession.builder()
                .appName("spark-kafka-sql-app")
                .getOrCreate();

        final Dataset<Row> load = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "benky-kafka:9092")
                .option("subscribe", "test")
                .load();

        final Dataset<Tuple2<String, String>> dataset = load.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .as(Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

        dataset.printSchema();

        dataset.writeStream()
                .outputMode("append")
                .format("console")
                .start()
                .awaitTermination();
    }
}
