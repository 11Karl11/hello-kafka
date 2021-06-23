package com.karl.one.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author karl xie
 */
public class ProducerInterceptorPrefixPlus implements ProducerInterceptor<String, String> {


    @Override
    public ProducerRecord<String, String> onSend(
            ProducerRecord<String, String> record) {
        String modifiedValue = "prefix2-" + record.value();
        return new ProducerRecord<>(record.topic(),
                record.partition(), record.timestamp(),
                record.key(), modifiedValue, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}