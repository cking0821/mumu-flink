package com.lovecws.mumu.flink.streaming.cep.kafka;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * kafka接受消息
 */
public class FlinkKafkaConsumer extends ShutdownableThread {

    private KafkaConsumer<Integer, String> consumer;
    private String topic;

    public FlinkKafkaConsumer(String brokerServers, String topic) {
        super("FlinkKafkaConsumer",false);
        this.topic = topic;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaQuickStartConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<Integer, String>(props);
    }

    @Override
    public void doWork() {
        consumer.subscribe(Arrays.asList(topic));
        ConsumerRecords<Integer, String> records = consumer.poll(1000);
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println("Received message: (" + record);
        }
    }
}
