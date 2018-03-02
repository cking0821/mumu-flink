package com.lovecws.mumu.flink.streaming.cep.kafka;

import com.lovecws.mumu.flink.MumuFlinkConfiguration;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: kafka streaming
 * @date 2018-03-02 9:23
 */
public class FlinkKafkaStreaming {

    /**
     * kafka streaming流
     *
     * @param brokerServer
     * @param topic
     * @throws Exception
     */
    public void streaming(String brokerServer, String topic) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = MumuFlinkConfiguration.streamExecutionEnvironment();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flinkKafkaConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        DataStream<String> streamSource = streamExecutionEnvironment.addSource(new FlinkKafkaConsumer09(topic, new SimpleStringSchema(), props));
        SingleOutputStreamOperator<Tuple2<String, Integer>> outputStreamOperator = streamSource
                .flatMap(new WordSplit())
                .keyBy(0)
                .sum(1);
        outputStreamOperator.print().setParallelism(1);
        streamExecutionEnvironment.execute("FlinkKafkaStreaming streaming");
    }

    /**
     * kafka带时间窗口的流
     *
     * @param brokerServer
     * @param topic
     * @throws Exception
     */
    public void windowStreaming(String brokerServer, String topic) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = MumuFlinkConfiguration.streamExecutionEnvironment();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flinkKafkaConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        DataStream<String> streamSource = streamExecutionEnvironment.addSource(new FlinkKafkaConsumer09(topic, new SimpleStringSchema(), props));
        streamSource.print().setParallelism(1);
        DataStream<Tuple2<String, Integer>> outputStreamOperator = streamSource
                .flatMap(new WordSplit())
                .keyBy(0)
                .timeWindow(Time.seconds(10), Time.seconds(1))
                .sum(1);
        outputStreamOperator.print().setParallelism(1);
        streamExecutionEnvironment.execute("FlinkKafkaStreaming windowStreaming");
    }

    /**
     * 将实时流数据发送到kafka消息组件中
     *
     * @param brokerServer
     * @param topic
     * @throws Exception
     */
    public void kafkaSink(String brokerServer, String topic) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = MumuFlinkConfiguration.streamExecutionEnvironment();

        //source
        DataStreamSource<String> dataStreamSource = streamExecutionEnvironment.fromElements("lovecws cws love lovecws cws love lovecws cws lovelovecws cws lovelovecws cws lovelovecws cws love");

        //transforms
        DataStream<Tuple2<String, Integer>> outputStreamOperator = dataStreamSource
                .flatMap(new WordSplit())
                .keyBy(0)
                .sum(1);
        DataStream<String> mapStream = outputStreamOperator.map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(final Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0 + ":" + tuple2.f1;
            }
        });

        //sink
        outputStreamOperator.addSink(new FlinkKafkaProducer09<Tuple2<String, Integer>>(brokerServer,
                topic,
                new TypeInformationKeyValueSerializationSchema<String, Integer>(TypeInformation.of(String.class), TypeInformation.of(Integer.class), new ExecutionConfig())));

        //TODO 这里的流数据必须和schema匹配 本来使用DataStream<Tuple2<String, Integer>> 来输出到kafka 因为类型问题 搞了一上午 而且从flink中找不到问题 就是报nullPointException
        mapStream.addSink(new FlinkKafkaProducer09<String>(brokerServer, topic, new KeyedSerializationSchemaWrapper(new SimpleStringSchema())));

        streamExecutionEnvironment.execute("FlinkKafkaStreaming kafkaSink");
    }

    public static class WordSplit implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(final String line, final Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : line.split("\\s+")) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
