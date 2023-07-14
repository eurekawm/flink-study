package com.zhyf.highlevel;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedProcessFunctionAndEventTime {
    public static void main(String[] args) throws Exception {
        // 1 创建一个入口环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(4000);
        DataStreamSource<String> streamSource = environment.socketTextStream("172.17.195.93", 9999);
        streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                String[] split = element.split(",");
                                return Long.parseLong(split[0]);
                            }
                        }))
                .map(item -> {
                    // 输入的是 1000,flink,1 第一个是时间
                    String[] split = item.split(",");
                    return Tuple2.of(split[1], Integer.parseInt(split[2]));
                }).returns(new TypeHint<Tuple2<String, Integer>>() {
                })
                .keyBy(tp -> tp.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

                    private ValueState<Integer> countState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
                        countState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 触发定时器的时候执行这个方法
                        // 输出数据
                        out.collect(Tuple2.of(ctx.getCurrentKey(), countState.value()));
                        // 由于滚动窗口是累加当前窗口的数据 那么要清空以前的数据
                        // 只清空当前key的对应的数据
                        countState.clear();
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Integer count = countState.value();
                        if (count == null) {
                            count = 0;
                        }
                        count += value.f1;
                        countState.update(count);

                        // 获取当前的waterMark
                        long waterMarkTime = ctx.timerService().currentProcessingTime();
                        long triggerTime = waterMarkTime + 10000;
                        if (waterMarkTime > Long.MIN_VALUE) {
                            // 注册定时器
                            // 当前的waterMark+10s
                            ctx.timerService().registerEventTimeTimer(triggerTime);
                        }
                    }
                }).print();
        environment.execute();
    }
}
