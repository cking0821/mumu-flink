package com.lovecws.mumu.flink.table;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: flink table
 * @date 2018-02-28 14:28
 */
public class WordCountTable {

    public void wordCount() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataSet<WC> input = env.fromElements(
                new WC("Hello", 1),
                new WC("Ciao", 1),
                new WC("Ciao", 2),
                new WC("cws", 3),
                new WC("lovecws", 1),
                new WC("Hello", 1));

        Table table = tEnv.fromDataSet(input);

        Table filtered = table
                .groupBy("word")
                .select("word, sum(frequency) as frequency")
                .orderBy("frequency");
        DataSet<WC> result = tEnv.toDataSet(filtered, WC.class);
        result.print();
    }

    public void sqlQuery() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataSet<WC> input = env.fromElements(
                new WC("Hello", 1),
                new WC("Ciao", 1),
                new WC("Ciao", 2),
                new WC("cws", 3),
                new WC("lovecws", 1),
                new WC("Hello", 1));
        tEnv.registerDataSet("WordCount", input, "word,frequency");
        Table table = tEnv.sqlQuery("SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word order by frequency desc");
        DataSet<WC> result = tEnv.toDataSet(table, WC.class);
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
