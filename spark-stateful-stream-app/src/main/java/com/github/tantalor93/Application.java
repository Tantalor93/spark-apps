package com.github.tantalor93;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Application {
    public static void main(String[] args) throws InterruptedException {
        final JavaStreamingContext streamingContext = new JavaStreamingContext(
                new SparkConf().setAppName("spark-stateful-stream-app"),
                Durations.seconds(30)
        );

        streamingContext.checkpoint("gs://benky-bucket/state-stream-app");

        final JavaDStream<String> linesStream = streamingContext
                .textFileStream("gs://benky-bucket/stream-app-input");

        linesStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair( word -> new Tuple2<>(word, 1))
                .updateStateByKey((List<Integer> values, Optional<Integer> state) -> {
                    int newState = values.stream().mapToInt(e -> e).sum() + state.orElse(0);
                    return Optional.of(newState);
                })
                .print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
