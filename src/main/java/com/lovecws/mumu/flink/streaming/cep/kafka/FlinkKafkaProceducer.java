package com.lovecws.mumu.flink.streaming.cep.kafka;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FlinkKafkaProceducer {

    private String topic;
    private KafkaProducer<Integer, String> producer = null;

    public FlinkKafkaProceducer(final String brokerServers, final String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerServers);
        props.put("client.id", "KafkaQuickStartProceducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("retries", 3);//重试次数
        producer = new KafkaProducer<Integer, String>(props);
    }

    /**
     * 发送同步消息
     *
     * @param message 消息
     * @param count   数量
     */
    public void sendMessage(String message, int count) {
        try {
            for (int i = 0; i < count; i++) {
                RecordMetadata metadata = producer.send(new ProducerRecord<Integer, String>(topic, 0, i, message)).get();
                System.out.println(DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss") + " : " + message);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 发送异步消息
     *
     * @param message 消息
     * @param count   数量
     */
    public void sendAsyncMessage(String message, int count) {
        try {
            for (int i = 0; i < count; i++) {
                Object o = producer.send(new ProducerRecord<Integer, String>(topic, null, message), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        System.out.println("message send end" + recordMetadata);
                    }
                }).get();
                System.out.println("send message:" + o);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }
}
