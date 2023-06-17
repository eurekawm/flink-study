package com.zhyf;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StreamCoGroup {
    public static void main(String[] args) throws Exception {
        // 1 创建一个入口环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
        // 2 通过source算子映射数据源为一个dataStream
        // id name
        DataStreamSource<String> streamSource1 = environment.socketTextStream("192.168.101.26", 9999);
        // id age city
        DataStreamSource<String> streamSource2 = environment.socketTextStream("192.168.101.26", 8888);

        SingleOutputStreamOperator<Tuple2<String, String>> s1 = streamSource1.map(s -> {
            String[] split = s.split(",");
            return Tuple2.of(split[0], split[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {
        });


        SingleOutputStreamOperator<Tuple3<String, String, String>> s2 = streamSource2.map(s -> {
            String[] split = s.split(",");
            return Tuple3.of(split[0], split[1], split[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });

        // coGroup
        // 两个流中的数据类型必须一致
        DataStream<String> result = s1.coGroup(s2).where(tp -> tp.f0) // 左流的f0
                .equalTo(tp -> tp.f0) // 右流的f0
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {

                    /**
                     *
                     * @param left 协同组中的左流的数据
                     * @param right 协同组中的右流的数据
                     * @param out   处理结果的输出器
                     */
                    @Override
                    public void coGroup(Iterable<Tuple2<String, String>> left, Iterable<Tuple3<String, String, String>> right, Collector<String> out) throws Exception {
                        // 这里实现 left out right
                        for (Tuple2<String, String> t1 : left) {
                            boolean flag = false;
                            for (Tuple3<String, String, String> t2 : right) {
                                out.collect(t1.f0 + "," + t1.f1 + "," + t2.f0 + "," + t2.f1 + "," + t2.f2);
                                flag = true;
                            }
                            // 这里是右表没有数据 就输出左表数据
                            if (!flag) {
                                out.collect(t1.f0 + "," + t1.f1 + "," + "null" + "," + "null" + "," + "null");
                            }
                        }
                    }
                });
        result.print();
        environment.execute();

    }
}
