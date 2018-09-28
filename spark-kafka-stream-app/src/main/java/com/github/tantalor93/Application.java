package com.github.tantalor93;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Pripoji se ke Kafce na broker benky-kafka:9092 k topicu test
 * a pak bude pocitat vyskyt znaku v prichazejicich zpravach
 */
public class Application {

    public static void main(String[] args) throws InterruptedException {
        final JavaStreamingContext streamingContext = new JavaStreamingContext(
                new SparkConf().setAppName("spark-stateful-stream-app"),
                Durations.seconds(10)
        );

        streamingContext.checkpoint("gs://benky-bucket/state-stream-app");

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
        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunction =
                (letter, count, state) -> {
            final int sum = count.orElse(0) + (state.exists()? state.get() : 0);
            state.update(sum);
            return new Tuple2<>(letter, sum);
        };
        inputStream.flatMap(e -> Arrays.asList(e._2.split("")).iterator())
                .mapToPair(character -> new Tuple2<>(character, 1))
                .mapWithState(StateSpec.function(mappingFunction))
                .print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
