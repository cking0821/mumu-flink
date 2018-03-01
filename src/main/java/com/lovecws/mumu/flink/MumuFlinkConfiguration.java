package com.lovecws.mumu.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: apache flink configuration
 * @date 2018-02-27 16:01
 */
public class MumuFlinkConfiguration {

    private static String FLINK_ENVIRONMENT = "local";

    public static ExecutionEnvironment executionEnvironment() {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        return executionEnvironment;
    }

    public static StreamExecutionEnvironment streamExecutionEnvironment() {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        return streamExecutionEnvironment;
    }
}
