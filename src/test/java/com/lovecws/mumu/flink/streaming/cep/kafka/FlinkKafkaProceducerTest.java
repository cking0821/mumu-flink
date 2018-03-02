package com.lovecws.mumu.flink.streaming.cep.kafka;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import java.util.Timer;
import java.util.TimerTask;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: kafka 消息生产者
 * @date 2018-03-02 9:55
 */
public class FlinkKafkaProceducerTest {

    private FlinkKafkaProceducer proceducer = new FlinkKafkaProceducer("192.168.11.25:9092", "babymm");

    private String[] data = new String[]{"love cws", "lovecws", "babymm", "youzi", "mumu love cws"};

    @Test
    public void sendMessage() {
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                proceducer.sendMessage(data[RandomUtils.nextInt(0, data.length)], 2);
            }
        }, 1000, 1000);
        while (true) {
            Thread.yield();
        }
    }

    @Test
    public void sendAsyncMessage() {
        proceducer.sendAsyncMessage("lovecws", 1000);
    }
}
