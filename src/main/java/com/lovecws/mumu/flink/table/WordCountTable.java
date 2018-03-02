package com.lovecws.mumu.flink.table;


import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: flink table
 * @date 2018-02-28 14:28
 */
public class WordCountTable {

    public void wordCount() throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(executionEnvironment);

        DataSet<WC> input = executionEnvironment.fromElements(
                new WC("Hello", 1),
                new WC("Ciao", 1),
                new WC("Ciao", 2),
                new WC("cws", 3),
                new WC("lovecws", 1),
                new WC("Hello", 1));

        Table table = tableEnvironment.fromDataSet(input);

        Table filtered = table
                .groupBy("word")
                .select("word, sum(frequency) as frequency")
                .orderBy("frequency");
        DataSet<WC> result = tableEnvironment.toDataSet(filtered, WC.class);
        result.print();
    }


    public void sqlQuery() throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(executionEnvironment);

        DataSet<WC> input = executionEnvironment.fromElements(
                new WC("Hello", 1),
                new WC("Ciao", 1),
                new WC("Ciao", 2),
                new WC("cws", 3),
                new WC("lovecws", 1),
                new WC("Hello", 1));
        tableEnvironment.registerDataSet("WordCount", input, "word,frequency");
        Table table = tableEnvironment.sqlQuery("SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word order by frequency desc");
        DataSet<WC> result = tableEnvironment.toDataSet(table, WC.class);
        result.print();
    }

    public void textFile(String filePath) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(executionEnvironment);
        DataSet<String> dataSet = executionEnvironment.readTextFile(filePath);
        FlatMapOperator<String, WC> flatMapOperator = dataSet.flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(final String line, final Collector<WC> collector) throws Exception {
                for (String word : line.split("\\s")) {
                    collector.collect(new WC(word, RandomUtils.nextInt(1, 10)));
                }
            }
        });
        Table table = tableEnvironment.fromDataSet(flatMapOperator);
        table.printSchema();

        Table filtered = table
                .groupBy("word")
                .select("word, sum(frequency) as frequency")
                .orderBy("frequency");
        DataSet<WC> result = tableEnvironment.toDataSet(filtered, WC.class);
        result.print();
    }

    public static class WC {
        public String word;
        public long frequency;

        public WC() {
        }

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return word + " " + frequency;
        }
    }
}
