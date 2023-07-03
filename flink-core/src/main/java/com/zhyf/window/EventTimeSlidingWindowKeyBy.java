package com.zhyf.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 滚动窗口 使用新APi
 */
public class EventTimeSlidingWindowKeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.socketTextStream("172.17.195.93", 9999);
        // 提取数据中的时间 eventTime
        // 该方法仅是提取数据中的eventTime 用来生成watermark 调用该方法 数据的格式不改变
        streamSource.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                                    @Override
                                    public long extractTimestamp(String s, long l) {
                                        String[] split = s.split(",");
                                        return Long.parseLong(split[0]);
                                    }
                                    // 安装系统时间设置超时时间
                                }).withIdleness(Duration.ofMinutes(10)))
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return Tuple2.of(split[1], Integer.parseInt(split[2]));
                    }
                    // 不keyby 划分eventTime类型的滚动窗口
                }).keyBy(f -> f.f1)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(1)
                .print();
        environment.execute();
    }
}
