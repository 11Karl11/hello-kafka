package com.karl.kafka_admin_client;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author karl xie
 */
public class TestKafkaAdminClient1 {

    public static void main(String[] args) {
        String brokerList =  "192.168.136.128:9092";
        String topic = "topic-admin11";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        NewTopic newTopic = new NewTopic(topic, 4, (short) 1);
        // Map<String, String> configs = new HashMap<>();
        // configs.put("cleanup.policy", "compact");
        // newTopic.configs(configs);

        CreateTopicsResult result = client.
                createTopics(Collections.singleton(newTopic));
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }
}