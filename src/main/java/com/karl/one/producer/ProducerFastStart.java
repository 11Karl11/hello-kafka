package com.karl.one.producer;

import com.karl.one.Company;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author karl xie
 */
public class ProducerFastStart {

    public static final String brokerList = "192.168.136.128:9092";
    public static final String topic = "topic-demo";


    public static Properties initConfig(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                CompanySerializer.class.getName());
                // StringSerializer.class.getName());
        props.put(ProducerConfig.RETRIES_CONFIG, 10);

        //自定义分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                DemoPartitioner.class.getName());

        //生产者拦截器（value是字符串，测试的时候需要改一下value的序列化方式）
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                ProducerInterceptorPrefix.class.getName() + ","
                        + ProducerInterceptorPrefixPlus.class.getName());
        return props;
    }

    public static void main(String[] args) {

        Properties properties = initConfig();
        // KafkaProducer<String, String> producer =
        KafkaProducer<String, Company> producer =
                new KafkaProducer<>(properties);


        Company company = Company.builder().name("karl")
                .address("China").build();
        ProducerRecord<String, Company> record =
                new ProducerRecord<>(topic, company);
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


        // ProducerRecord<String, String> record =
        //         new ProducerRecord<>(topic, "Hello, Kafka!");


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
        producer.close();
    }
}