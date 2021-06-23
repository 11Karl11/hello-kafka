package com.karl.kafka_admin_client;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author karl xie
 */
public class TestKafkaAdminClient2 {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String brokerList = "192.168.136.128:9092";
        String topic = "topic-admin";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        ConfigEntry entry = new ConfigEntry("cleanup.policy", "compact");
        Config config = new Config(Collections.singleton(entry));
        Map<ConfigResource, Config> configs = new HashMap<>();
        configs.put(resource, config);
        AlterConfigsResult result = client.alterConfigs(configs);
        result.all().get();
        client.close();
    }
}