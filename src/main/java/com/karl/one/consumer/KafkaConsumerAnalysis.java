package com.karl.one.consumer;

import com.karl.one.Company;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 通过 subscribe() 方法订阅主题具有消费者自动再均衡的功能，在多个消费者的情况下可以根据分区分配策略来自动分配各个消费者与分区的关系。
 * 当消费组内的消费者增加或减少时，分区分配关系会自动调整，以实现消费负载均衡及故障自动转移。而通过 assign() 方法订阅分区时，
 * 是不具备消费者自动均衡的功能的，其实这一点从 assign() 方法的参数中就可以看出端倪，
 * 两种类型的 subscribe() 都有 ConsumerRebalanceListener 类型参数的方法，而 assign() 方法却没有。
 *
 * @author karl xie
 */
@Slf4j
public class KafkaConsumerAnalysis {
    public static final String brokerList = "192.168.136.128:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        // props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        //         StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                CompanyDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client.id.demo");

        //手动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, Company> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        //只订阅 topic-demo 主题中分区编号为0的分区
        // consumer.assign(Arrays.asList(new TopicPartition("topic-demo", 0)));
      /*  List<TopicPartition> partitions = new ArrayList<>();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos != null) {
            for (PartitionInfo tpInfo : partitionInfos) {
                partitions.add(new TopicPartition(tpInfo.topic(), tpInfo.partition()));
            }
        }
        consumer.assign(partitions);*/
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, Company> records =
                        consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Company> record : records) {
                    System.out.println("topic = " + record.topic()
                            + ", partition = " + record.partition()
                            + ", offset = " + record.offset());
                    System.out.println("key = " + record.key()
                            + ", value = " + record.value());
                    //do something to process record.
                }
            }
        } catch (Exception e) {
            log.error("occur exception ", e);
        } finally {
            // consumer.close();
        }

        //如果将 subscribe(Collection) 或 assign(Collection) 中的集合参数设置为空集合，那么作用等同于 unsubscribe() 方法，下面示例中的三行代码的效果相同：
        // consumer.unsubscribe();//取消订阅
        // consumer.subscribe(new ArrayList<String>());
        // consumer.assign(new ArrayList<TopicPartition>());
    }
}