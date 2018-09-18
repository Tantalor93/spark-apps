package com.github.tantalor93;


import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class Application {
    public static void main(String[] args) throws InterruptedException {
        final JavaStreamingContext streamingContext = new JavaStreamingContext(
                new SparkConf().setAppName("spark-stream-app"),
                Durations.seconds(5)
        );

        final JavaDStream<String> linesStream = streamingContext
                .textFileStream("gs://benky-bucket/stream-app-input");

        linesStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair( word -> new Tuple2<>(word, 1))
                .reduceByKey((i1, i2) -> i1 + i2)
                .print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
