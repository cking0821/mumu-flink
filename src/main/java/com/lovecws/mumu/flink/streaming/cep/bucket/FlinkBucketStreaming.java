package com.lovecws.mumu.flink.streaming.cep.bucket;

import com.lovecws.mumu.flink.MumuFlinkConfiguration;
import com.lovecws.mumu.flink.streaming.cep.kafka.FlinkKafkaStreaming;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.AvroKeyValueSinkWriter;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: hadoop bucket sink
 * @date 2018-03-02 11:58
 */
public class FlinkBucketStreaming {

    public void bucket(List<String> elements, String output, String fileType) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = MumuFlinkConfiguration.streamExecutionEnvironment();

        //source
        DataStreamSource<String> dataStreamSource = streamExecutionEnvironment.fromCollection(elements);

        //transforms
        DataStream<Tuple2<String, Integer>> outputStreamOperator = dataStreamSource
                .flatMap(new FlinkKafkaStreaming.WordSplit())
                .keyBy(0)
                .sum(1);

        //sink
        if (fileType == null || "string".equalsIgnoreCase(fileType)) {
            //简单字符串输出
            SingleOutputStreamOperator<String> mapStreamOperator = outputStreamOperator.map(new MapFunction<Tuple2<String, Integer>, String>() {
                @Override
                public String map(final Tuple2<String, Integer> tuple2) throws Exception {
                    return tuple2.f0 + ":" + tuple2.f1;
                }
            });
            BucketingSink<String> bucketingSink = new BucketingSink<String>(output);
            bucketingSink.setBucketer(new DateTimeBucketer<>("yyyyMMddHHmmss"));
            bucketingSink.setBatchSize(1024 * 1024);
            bucketingSink.setWriter(new StringWriter<>());
            mapStreamOperator.addSink(bucketingSink);
        } else if ("sequenceFile".equalsIgnoreCase(fileType)) {
            SingleOutputStreamOperator<Tuple2<Text, IntWritable>> streamOperator = outputStreamOperator.map(new MapFunction<Tuple2<String, Integer>, Tuple2<Text, IntWritable>>() {
                @Override
                public Tuple2<Text, IntWritable> map(final Tuple2<String, Integer> tuple2) throws Exception {
                    return new Tuple2<Text, IntWritable>(new Text(tuple2.f0), new IntWritable(tuple2.f1));
                }
            });
            BucketingSink<Tuple2<Text, IntWritable>> sequenceFileSink = new BucketingSink<Tuple2<Text, IntWritable>>(output);
            sequenceFileSink.setBucketer(new DateTimeBucketer<>("yyyyMMddHHmmss"));
            sequenceFileSink.setBatchSize(1024 * 1024);
            SequenceFileWriter<Text, IntWritable> sequenceFileWriter = new SequenceFileWriter<Text, IntWritable>();
            sequenceFileWriter.setInputType(TypeInformation.of(Tuple2.class), new ExecutionConfig());
            sequenceFileSink.setWriter(sequenceFileWriter);
            streamOperator.addSink(sequenceFileSink);
        } else if ("avro".equalsIgnoreCase(fileType)) {
            BucketingSink<Tuple2<String, Integer>> avroSink = new BucketingSink<Tuple2<String, Integer>>(output);
            avroSink.setBucketer(new DateTimeBucketer<>("yyyyMMddHHmmss"));
            avroSink.setBatchSize(1024 * 1024);

            Map<String, String> properties = new HashMap<String, String>();
            properties.put("avro.schema.output.key", "{key:}");
            properties.put("avro.schema.output.value", "java.lang.Integer");
            AvroKeyValueSinkWriter avroKeyValueSinkWriter = new AvroKeyValueSinkWriter(properties);
            avroKeyValueSinkWriter.setInputType(TypeInformation.of(Tuple2.class), new ExecutionConfig());
            avroSink.setWriter(avroKeyValueSinkWriter);
            outputStreamOperator.addSink(avroSink);
        }
        streamExecutionEnvironment.execute("FlinkBucketStreaming bucket");
    }
}
