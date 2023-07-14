package com.zhyf.state;

import com.google.errorprone.annotations.Var;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 累加增量聚合 窗口关闭的时候再和历史数据聚合
 */
public class ReduceHistory {
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
                }).keyBy(f -> f.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new MyWindowReduceFunction(), new MyWindowFunction())
                .print();
        environment.execute();
    }

    // 增量聚合 窗口没触发的时候就执行了
    public static class MyWindowReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tp1, Tuple2<String, Integer> tp2) throws Exception {
            tp1.f1 += tp2.f1;
            return tp1;
        }
    }

    // 窗口触发的时候触发 把窗口内聚合的结果的输入作为输入
    public static class MyWindowFunction extends RichWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 状态存历史数据
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            countState = getRuntimeContext().getState(stateDescriptor);
        }

        // 窗口触发后调用此处
        @Override
        public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 获取窗口
            Tuple2<String, Integer> tp2 = input.iterator().next();
            Integer count = countState.value();
            if (count == null) {
                count = 0;
            }
            count += tp2.f1;
            countState.update(count);
            tp2.f1 = count;
            out.collect(tp2);
        }
    }
}
