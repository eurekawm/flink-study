package com.zhyf.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 对窗口中的数据进行聚合操作 更加灵活
 * 划分窗口后调用reduce max min 。。。使用的是增量聚合
 * 即使窗口没有触发输入相同key的数据 也会进行增量技术 窗口触发后输出结果
 * 优点 节省资源
 */
public class WindowReduce {
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

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = windowedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                t1.f1 += t2.f1;
                return t1;
            }
        });

        reduce.print();

        environment.execute();
    }
}
