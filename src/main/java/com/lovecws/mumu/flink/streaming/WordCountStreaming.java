package com.lovecws.mumu.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

public class WordCountStreaming {

    /**
     * 从数据流中计算单词数量
     *
     * @param dataStream
     */
    private void wordCount(DataStream<String> dataStream, boolean window) {
        dataStream.print();
        SingleOutputStreamOperator<Tuple2<String, Long>> outputStream = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(final String line, final Collector<Tuple2<String, Long>> collector) throws Exception {
                for (String word : line.split("\\s+")) {
                    collector.collect(new Tuple2<>(word.trim(), 1l));
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> reduceStream = null;
        if (window) {
            reduceStream = outputStream.keyBy(0)
                    //.countWindow(5)
                    .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                        @Override
                        public Tuple2<String, Long> reduce(final Tuple2<String, Long> tuple2, final Tuple2<String, Long> tuple22) throws Exception {
                            return new Tuple2<>(tuple2.f0, tuple2.f1 + tuple22.f1);
                        }
                    });
        } else {
            reduceStream = outputStream.keyBy(0).sum(1);
        }
        reduceStream.print().setParallelism(1);
    }

    /**
     * 只读取文件一次
     *
     * @param filePath
     */
    public void file(String filePath) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.readTextFile(filePath);
        wordCount(dataStream, false);
        env.execute("file WordCount");
    }

    /**
     * 持续监控一个文件 如果只是监控文件的生成 则没有问题 如果监控文件持续添加 则会出现重复问题
     * @param filePath
     */
    public void continuouslyFile(String filePath) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.readFile(new TextInputFormat(new Path(filePath)),
                filePath,
                FileProcessingMode.PROCESS_CONTINUOUSLY,
                1000);
        wordCount(dataStream, true);
        env.execute("continuously file Window WordCount");
    }

    /**
     * @param hostname 192.168.11.25
     * @param port     9999
     */
    public void socket(String hostname, int port) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.socketTextStream(hostname, port, "\n");
        wordCount(dataStream, true);
        env.execute("Socket Window WordCount");
    }

    public void collection(String... collections) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.fromElements(collections);
        wordCount(dataStream, false);
        env.execute("Socket Window WordCount");
    }

    public static void main(String[] args) throws Exception {
        new WordCountStreaming().continuouslyFile("E:\\\\mumu\\\\flink\\\\streaming");
    }
}