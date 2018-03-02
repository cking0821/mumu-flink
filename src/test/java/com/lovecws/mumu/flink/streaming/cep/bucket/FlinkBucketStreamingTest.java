package com.lovecws.mumu.flink.streaming.cep.bucket;

import org.junit.Test;

import java.util.Arrays;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: bucket
 * @date 2018-03-02 13:37
 */
public class FlinkBucketStreamingTest {

    @Test
    public void bucket() throws Exception {
        new FlinkBucketStreaming().bucket(Arrays.asList("lovecws cws love lovecws cws love lovecws cws lovelovecws cws lovelovecws cws lovelovecws cws love" +
                        "lovecws cws love lovecws cws love lovecws cws lovelovecws cws lovelovecws cws lovelovecws cws love" +
                        "lovecws cws love lovecws cws love lovecws cws lovelovecws cws lovelovecws cws lovelovecws cws love" +
                        "lovecws cws love lovecws cws love lovecws cws lovelovecws cws lovelovecws cws lovelovecws cws love" +
                        "lovecws cws love lovecws cws love lovecws cws lovelovecws cws lovelovecws cws lovelovecws cws love" +
                        "lovecws cws love lovecws cws love lovecws cws lovelovecws cws lovelovecws cws lovelovecws cws love" +
                        "lovecws cws love lovecws cws love lovecws cws lovelovecws cws lovelovecws cws lovelovecws cws love" +
                        "lovecws cws love lovecws cws love lovecws cws lovelovecws cws lovelovecws cws lovelovecws cws love" +
                        "lovecws cws love lovecws cws love lovecws cws lovelovecws cws lovelovecws cws lovelovecws cws love"),
                "hdfs://192.168.11.25:9000/mumu/flink/bucket",
                "sequenceFile");
    }
}
