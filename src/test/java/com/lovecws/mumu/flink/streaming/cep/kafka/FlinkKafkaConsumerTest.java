package com.lovecws.mumu.flink.streaming.cep.kafka;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: flink kafka 消费者
 * @date 2018-03-02 9:52
 */
public class FlinkKafkaConsumerTest {

    @Test
    public void receiveMessage() {
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("", "");
        consumer.start();
        try {
            TimeUnit.SECONDS.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
