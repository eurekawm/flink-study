package com.zhyf.flinksql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQLWordCount {

    public static void main(String[] args) throws Exception {
        // 1 创建一个入口环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment SQLEnv = StreamTableEnvironment.create(environment);
        DataStreamSource<String> socketSource = environment.socketTextStream("172.17.195.93", 9999);
        // flink,2
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapedStream = socketSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0], Integer.parseInt(split[1]));
            }
        });

        SQLEnv.createTemporaryView("v_data", mapedStream);

        //"select f0 word, sum(f1) from v_data group by f0
        TableResult tableResult = SQLEnv.executeSql("desc v_data");

        tableResult.print();
        environment.execute();


    }
}
