package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KafkaConsumer {
    public static void main(String[] args) throws InterruptedException {
        String topic = "kafkaTopic";
        String broker = "kafkabroker";
        String group = "kafkaGroup";

        SparkConf sparkConf = new SparkConf()
                .set("spark.ui.port", "4069");

        SparkSession sparkSession = SparkSession.builder()
                .master("local[2]")
                .config(sparkConf)
                .getOrCreate();


        JavaStreamingContext jsc = new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()),
                Durations.seconds(10));

        String[] tmpTopics = topic.split(",");
        Collection<String> topics = Arrays.asList(tmpTopics);
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", broker);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", group);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        JavaInputDStream<ConsumerRecord<String, String>> msg = KafkaUtils.createDirectStream(jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        msg.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            public void call(JavaRDD<ConsumerRecord<String, String>> javaRDD) throws Exception {
                javaRDD.foreach(new VoidFunction<ConsumerRecord<String, String>>() {
                    public void call(ConsumerRecord<String, String> record) throws Exception {
                        System.out.println(record.value());
                    }
                });
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
