package com.karl.demo.product;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author karl xie
 */
public class KafkaProducerAnalysis {

    public static final String brokerList = "192.168.136.128:9092";
    public static final String topic = "topic-demo";

    public static Properties initConfig(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.RETRIES_CONFIG, 10);
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, "Hello, Kafka!");
        // try {
        //     Future<RecordMetadata> future = producer.send(record);
        //     RecordMetadata metadata = future.get();
        //     System.out.println(metadata.topic() + "-" +
        //             metadata.partition() + ":" + metadata.offset());
        // } catch (ExecutionException | InterruptedException e) {
        //     e.printStackTrace();
        // }

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            } else {
                System.out.println(metadata.topic() + "-" +
                        metadata.partition() + ":" + metadata.offset());
            }
        });
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}