package com.lovecws.mumu.flink.streaming.cep.kafka;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: flink streaming
 * @date 2018-03-02 10:01
 */
public class FlinkKafkaStreamingTest {


    @Test
    public void streaming() throws Exception {
        new FlinkKafkaStreaming().streaming("192.168.11.25:9092", "babymm");
    }

    @Test
    public void windowStreaming() throws Exception {
        new FlinkKafkaStreaming().windowStreaming("192.168.11.25:9092", "babymm");
    }

    @Test
    public void kafkaSink() throws Exception {
        new FlinkKafkaStreaming().kafkaSink("192.168.11.25:9092", "babymm");
    }
}
