package com.github.tantalor93;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class Application {

    public static void main(String[] args) {
        final JavaStreamingContext streamingContext = new JavaStreamingContext(
                new SparkConf().setAppName("spark-stateful-stream-app"),
                Durations.seconds(10)
        );

        final Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "benky-kafka:9092");

        final JavaPairInputDStream<String, String> inputStream = KafkaUtils.createDirectStream(
                streamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                new HashSet<>(Arrays.asList("test"))
        );
        inputStream.flatMap(e -> Arrays.asList(e._2.split("")).iterator())
                .mapToPair(character -> new Tuple2<>(character, 1));

        //TODO update globalniho stavu, pocitat pismena v prichazejicih zpravach
    }
}
