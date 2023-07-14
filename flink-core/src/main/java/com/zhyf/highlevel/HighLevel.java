package com.zhyf.highlevel;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class HighLevel {
    public static void main(String[] args) throws Exception {
        // 1 创建一个入口环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.socketTextStream("172.17.195.93", 9999);
        // 只输出偶数
        streamSource.process(new ProcessFunction<String, Integer>() {
            @Override
            public void processElement(String value, ProcessFunction<String, Integer>.Context ctx, Collector<Integer> out) throws Exception {
                try {
                    int i = Integer.parseInt(value);
                    if (i % 2 == 0) {
                        out.collect(i);
                    }
                } catch (NumberFormatException e) {
                    // TODO
                }
            }
        }).print();
        environment.execute();
    }

}
