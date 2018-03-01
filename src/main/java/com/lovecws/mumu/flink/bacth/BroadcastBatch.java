package com.lovecws.mumu.flink.bacth;

import com.lovecws.mumu.flink.MumuFlinkConfiguration;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 广播变量 广播数据集，就是有些不大的公共数据，是要被所有的实例访问到的，比如一些查询表,(不大的公共数据) 上面的例子，会将toBroadcast设置为广播变量broadcastSetName，这样在运行时，可以用getRuntimeContext().getBroadcastVariable获取该变量使用
 * @date 2018-03-01 9:53
 */
public class BroadcastBatch {

    public void broadcast(String... elements) throws Exception {
        ExecutionEnvironment executionEnvironment = MumuFlinkConfiguration.executionEnvironment();
        DataSet<String> dataSet = executionEnvironment.fromElements(elements);
        FlatMapOperator<String, Tuple2<String, Integer>> broadCastMap = dataSet.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            private List<Integer> broadcastSet;

            @Override
            public void open(final Configuration parameters) throws Exception {
                broadcastSet = getRuntimeContext().getBroadcastVariable("broadCastSet");
                super.open(parameters);
                Accumulator<Object, Serializable> accumulator = getRuntimeContext().getAccumulator("acc");
            }

            @Override
            public void flatMap(final String line, final Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : line.split("\\s")) {
                    collector.collect(new Tuple2<>(word, broadcastSet.get(RandomUtils.nextInt(0, broadcastSet.size()))));
                }
            }
        });

        DataSet<Integer> toBroadcast = executionEnvironment.fromElements(1, 2, 3);
        broadCastMap.withBroadcastSet(toBroadcast, "broadCastSet");
        
        AggregateOperator<Tuple2<String, Integer>> aggregateOperator = broadCastMap.groupBy(0).sum(1);
        aggregateOperator.print();
    }
}
