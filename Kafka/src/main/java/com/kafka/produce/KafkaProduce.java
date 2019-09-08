package com.kafka.produce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProduce {
    public static void main(String[] args) {
        String topic = "kafkaTopic";
        String broker = "kafkabroker";
        String group = "kafkaGroup";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", broker);
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);

        KafkaProducer<String, String> prod = new KafkaProducer<String, String>(properties);
        prod.send(new ProducerRecord<String, String>(topic, "data"));

        prod.close();
    }
}
