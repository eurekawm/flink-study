package com.zhyf.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


import java.util.HashSet;

/**
 * 输入数据
 * 用户id,行为id,广告id
 * u01,view,a01
 * u01,view,a01
 * u01,click,a01
 * u01,view,a02
 * u01,view,a02
 * u03,view,a02
 * 输出数据
 * 广告id 事件id, 统计人数, 统计次数
 * 2> (a01,view,1,1)
 * 2> (a01,view,1,2)
 * 7> (a01,click,1,1)
 * 10> (a02,view,1,1)
 * 10> (a02,view,1,2)
 * 10> (a02,view,2,3)
 */
public class C04_AdCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启checkpoint
        environment.enableCheckpointing(100000);

        DataStreamSource<String> dataStreamSource = environment.socketTextStream("172.17.195.93", 9999);

        SingleOutputStreamOperator<Tuple3<String, String, String>> map = dataStreamSource.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String s) throws Exception {
                String[] split = s.split(",");
                // 广告id 事件id 用户id
                return Tuple3.of(split[2], split[1], split[0]);
            }
        });

        KeyedStream<Tuple3<String, String, String>, Tuple2<String, String>> keyedStream = map.keyBy(new KeySelector<Tuple3<String, String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, String> tp) throws Exception {
                return Tuple2.of(tp.f0, tp.f1);
            }
        });

        // 用keyedState实现特定的功能
        SingleOutputStreamOperator<Tuple4<String, String, Integer, Integer>> process = keyedStream.process(new AdCountProcessFunction());

        process.print();

        environment.execute();

    }

    public static class AdCountProcessFunction extends KeyedProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<String, String, Integer, Integer>> {

        private transient ValueState<Integer> countState;
        private transient ValueState<HashSet<String>> uidState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化或恢复状态
            // 保存次数的state
            ValueStateDescriptor<Integer> countStateDes = new ValueStateDescriptor<>("count-state", Integer.class);
            countState = getRuntimeContext().getState(countStateDes);
            // 保存用户ID的状态
            ValueStateDescriptor<HashSet<String>> uidStateDesc = new ValueStateDescriptor<>("uid-state", TypeInformation.of(new TypeHint<HashSet<String>>() {
            }));
            uidState = getRuntimeContext().getState(uidStateDesc);
        }

        @Override
        public void processElement(Tuple3<String, String, String> value, KeyedProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<String, String, Integer, Integer>>.Context ctx, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {
            Integer count = countState.value();
            if (count == null) {
                count = 0;
            }

            count += 1;
            countState.update(count);
            HashSet<String> uidSet = uidState.value();

            // 统计人数
            if (uidSet == null) {
                uidSet = new HashSet<>();
            }
            uidSet.add(value.f2);
            uidState.update(uidSet);
            // 输出结果 广告id 时间id 人数 点击数
            out.collect(Tuple4.of(value.f0, value.f1, uidSet.size(), count));
        }

    }
}
