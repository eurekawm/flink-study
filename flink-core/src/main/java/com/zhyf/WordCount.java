package com.zhyf;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        // 1 创建一个入口环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
        // 2 通过source算子映射数据源为一个dataStream
        DataStreamSource<String> dataStreamSource = environment.socketTextStream("192.168.101.26", 9999);
        // 通过各种算子对数据进行操作
        // 拆分单词
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split("\\s+");
                for (String word : split) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
        // 分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tuple2SingleOutputStreamOperator.keyBy((KeySelector<Tuple2<String, Integer>, String>) tuple2 -> tuple2.f0);
        // 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = keyedStream.sum("f1");


        //3 sink算子把结果输出
        resultStream.print("sink_");
        // 4 触发
        environment.execute();
    }
}
