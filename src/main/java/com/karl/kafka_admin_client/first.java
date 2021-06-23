package com.karl.kafka_admin_client;

import kafka.admin.TopicCommand;

/**
 * 可以看到这种方式与使用 kafka-topics.sh 脚本的方式并无太大差别，可以使用这种方式集成到自动化管理系统中来创建相应的主题。
 * 当然这种方式也可以适用于对主题的删、改、查等操作的实现，只需修改对应的参数即可。不过更推荐使用KafkaAdminClient 来代替这种实现方式。
 * @author karl xie
 */
public class first {

    public static void createTopic(){
        String[] options = new String[]{
                "--zookeeper", "192.168.136.128:2181/kafka",
                "--create",
                "--replication-factor", "1",
                "--partitions", "1",
                "--topic", "topic-create-api"
        };
        TopicCommand.main(options);
    }

    public static void main(String[] args) {
        createTopic();
    }

}