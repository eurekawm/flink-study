package com.zhyf;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamUnion {
    public static void main(String[] args) throws Exception {
        // 相同的类型的流才能联合 比如两个字符流可以联合 但是一个字符流和一个数字流就不能联合
        // 1 创建一个入口环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
        // 2 通过source算子映射数据源为一个dataStream
        DataStreamSource<String> streamSource1 = environment.socketTextStream("192.168.101.26", 9999);
        DataStreamSource<String> streamSource2 = environment.socketTextStream("192.168.101.26", 8888);
        streamSource1.union(streamSource2).map(System.out::printf);
        environment.execute();
    }
}
