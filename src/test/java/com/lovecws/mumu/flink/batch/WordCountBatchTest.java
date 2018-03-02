package com.lovecws.mumu.flink.batch;

import com.lovecws.mumu.flink.bacth.WordCountBatch;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: batch
 * @date 2018-02-28 15:29
 */
public class WordCountBatchTest {

    private WordCountBatch wordCountBatch = new WordCountBatch();

    @Test
    public void textFile() throws Exception {
        wordCountBatch.textFile("hdfs://192.168.11.25:9000/mumu/spark/file");
    }

    @Test
    public void fromElements() throws Exception {
        wordCountBatch.fromElements("lovecws", "love", "babymm  lovecws","love  ");
    }
}
