package com.zhyf.state;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AdCountV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启checkpoint
        environment.enableCheckpointing(100000);
        DataStreamSource<String> dataStreamSource = environment.socketTextStream("172.17.195.93", 9999);
        dataStreamSource.map(item -> {
                    String[] split = item.split(",");
                    String time = split[0];
                    return Tuple3.of(time.substring(0, 13), split[1], split[2]);
                }).returns(TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {}))
                .keyBy(new KeySelector<Tuple3<String, String, String>, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(Tuple3<String, String, String> val) throws Exception {
                        return val;
                    }
                })
                .process(new AdProcessFunction())
                // 把结果写入redis

                .print();
        environment.execute();
    }

    public static class AdProcessFunction extends KeyedProcessFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple4<String, String, String, Integer>> {

        private transient ValueState<Integer> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2)).build();
            ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            valueStateDescriptor.enableTimeToLive(ttlConfig);
            valueState = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void processElement(Tuple3<String, String, String> value, KeyedProcessFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple4<String, String, String, Integer>>.Context ctx, Collector<Tuple4<String, String, String, Integer>> out) throws Exception {
            Integer count = valueState.value();
            if (count == null) {
                count = 0;
            }
            count += 1;
            valueState.update(count);
            out.collect(Tuple4.of(value.f0, value.f1, value.f2, count));
        }
    }
}
