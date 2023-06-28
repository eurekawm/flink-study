package com.zhyf.window;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 触发条件 当一个组内的数据达到了指定的条数 这个组单独触发 不影响其他的组
 */
public class KeyedCountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.socketTextStream("172.17.195.93", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = streamSource.map(item -> {
            String[] split = item.split(",");
            return Tuple2.of(split[0], Integer.parseInt(split[1]));
        }).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = operator.keyBy(t -> t.f0);

        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> windowedStream = keyedStream.countWindow(5);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.sum(1);

        sum.print();

        environment.execute();

    }
}
