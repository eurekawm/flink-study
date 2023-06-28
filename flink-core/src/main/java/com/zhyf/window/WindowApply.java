package com.zhyf.window;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 对窗口中的数据进行聚合操作 更加灵活
 * 把数据存起来，窗口触发之后 调用apply进行计算
 * 优点 节省资源
 */
public class WindowApply {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.socketTextStream("172.17.195.93", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = streamSource.map(item -> {
            String[] split = item.split(",");
            return Tuple2.of(split[0], Integer.parseInt(split[1]));
        }).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = operator.keyBy(t -> t.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> apply = windowedStream.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
            /**
             *
             * @param key The key for which this window is evaluated.
             * @param window The window that is being evaluated.
             * @param input The elements in the window being evaluated.
             * @param out A collector for emitting elements.
             * @throws Exception
             */
            @Override
            public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                int count = 0;
                for (Tuple2<String, Integer> tp : input) {
                    count += tp.f1;
                }
                out.collect(Tuple2.of(key, count));
            }
        });

        apply.print();
        environment.execute();
    }
}
