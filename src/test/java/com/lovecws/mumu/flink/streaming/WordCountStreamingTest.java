package com.lovecws.mumu.flink.streaming;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 字数统计
 * @date 2018-02-28 11:44
 */
public class WordCountStreamingTest {
    private WordCountStreaming wordCountStreaming = new WordCountStreaming();

    @Test
    public void file() throws Exception {
        wordCountStreaming.file("hdfs://192.168.11.25:9000/mumu/spark/file");
    }

    @Test
    public void continuouslyFile() throws Exception {
        wordCountStreaming.continuouslyFile("E:\\\\mumu\\\\flink\\\\streaming");
    }

    @Test
    public void socket() throws Exception {
        wordCountStreaming.socket("192.168.11.25", 9999);
    }

    @Test
    public void collection() throws Exception {
        wordCountStreaming.collection("lovecws", "love", "babymm  lovecws","love  ");
    }
}
