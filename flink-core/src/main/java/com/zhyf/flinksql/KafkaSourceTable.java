package com.zhyf.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用编程的方式创建kafka source表
 */
public class KafkaSourceTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 创建一个source表 即指定以后从kafka中读取数据 然后将kafka中的数据关联schema 映射成表

    }
}
