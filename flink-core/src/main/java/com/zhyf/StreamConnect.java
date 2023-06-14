package com.zhyf;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class StreamConnect {
    public static void main(String[] args) throws Exception {
        // 1 创建一个入口环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
        // 2 通过source算子映射数据源为一个dataStream
        DataStreamSource<String> streamSource1 = environment.socketTextStream("192.168.101.26", 9999);
        DataStreamSource<String> streamSource2 = environment.socketTextStream("192.168.101.26", 8888);

        // 两个流 绑定在一起了
        ConnectedStreams<String, String> connectedStreams = streamSource1.connect(streamSource2);

        SingleOutputStreamOperator<String> resultStream = connectedStreams.map(new CoMapFunction<String, String, String>() {

            String prefix = "hello_";

            @Override
            public String map1(String value) throws Exception {
                // 处理左流
                // 数字*10 返回

                return prefix + Integer.parseInt(value) * 10 + "";
            }

            @Override
            public String map2(String value) throws Exception {
                // 处理右流
                return prefix + Integer.parseInt(value) * 2 + "";
            }
        });
        resultStream.print();
        environment.execute();
    }
}
