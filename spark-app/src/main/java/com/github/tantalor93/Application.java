package com.github.tantalor93;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Application {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("spark-app"));
        JavaRDD<String> textFile = sparkContext.textFile("gs://benky-bucket/benky-text-file.txt");

        textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .saveAsTextFile("gs://benky-bucket/benky-spark-output");

        sparkContext.stop();
    }
}
