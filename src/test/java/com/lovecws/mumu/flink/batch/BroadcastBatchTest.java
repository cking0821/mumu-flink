package com.lovecws.mumu.flink.batch;

import com.lovecws.mumu.flink.bacth.BroadcastBatch;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 广播变量
 * @date 2018-03-01 10:11
 */
public class BroadcastBatchTest {

    private BroadcastBatch broadcastBatch = new BroadcastBatch();

    @Test
    public void broadcast() throws Exception {
        broadcastBatch.broadcast("lovecws love", "love", "love cws", "cws cws");
    }
}
