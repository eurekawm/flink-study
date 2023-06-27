package com.zhyf;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessFunctions {
    public static void main(String[] args) {
        // 1 创建一个入口环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
        // 2 通过source算子映射数据源为一个dataStream
        // id eventId
        DataStreamSource<String> streamSource1 = environment.socketTextStream("192.168.101.26", 9999);
        // id age city
        DataStreamSource<String> streamSource2 = environment.socketTextStream("192.168.101.26", 8888);

        streamSource1.process(new ProcessFunction<String, Tuple2<String, String>>() {
            /**
             * w
             * @param value 元素
             * @param ctx 上下问
             * @param out 收集器
             * @throws Exception
             */
            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                
                String[] split = value.split(",");
                out .collect(Tuple2.of(split[0], split[1]));
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, String>> s2 = streamSource2.map(s -> {
            String[] split = s.split(",");
            return Tuple3.of(split[0], split[1], split[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });
    }
}
