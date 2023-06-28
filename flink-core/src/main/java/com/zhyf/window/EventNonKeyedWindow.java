package com.zhyf.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 安装eventTime划分窗口
 */
public class EventNonKeyedWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.socketTextStream("172.17.195.93", 9999);
        // 提取数据中的时间 eventTime
        // 该方法仅是提取数据中的eventTime 用来生成watermark 调用该方法 数据的格式不改变
        streamSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String element) {
                        String[] split = element.split(",");
                        return Long.parseLong(split[0]);
                    }
                }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String,Integer> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return Tuple2.of(split[1], Integer.parseInt(split[2]));
                    }
                    // 不keyby 划分eventTime类型的滚动窗口
                }).keyBy(f->f.f1)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
                .sum(1)
                .print();
        environment.execute();
    }
}
