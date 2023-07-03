package com.zhyf.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class C03_CheckPoint {
    public static void main(String[] args) throws Exception {
        // 1 创建一个入口环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
        // 2 通过source算子映射数据源为一个dataStream
        DataStreamSource<String> dataStreamSource = environment.socketTextStream("172.17.195.93", 9999);
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
        // 不使用sum reduce等方法自己来实现功能
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = keyedStream.map(new ValueStateFunction());
        //3 sink算子把结果输出
        resultStream.print("sink_");
        // 4 触发
        environment.execute();
    }

    public static class ValueStateFunction extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化或者恢复状态
            // 使用状态的步骤
            // 1，定义状态描述器
            ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            // 2.使用运行时上下文 初始化或回复状态
            countState = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> tuple2) throws Exception {
            // 取出该单词的次数
            Integer count = countState.value();
            if (count == null) {
                count = 0;
            }
            count += tuple2.f1;
            countState.update(count);
            tuple2.f1 = count;
            return tuple2;
        }
    }
}
