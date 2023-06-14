package com.zhyf;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamJoin {
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

        /**
         * left -> id name
         * right -> id age city
         * join 算子 来实现两个流按id关联
         */
        // 为什么 equalTo后面只有window? 每一步操作之后返回的特定的对象 这些特定的对象之后只有特定的方法执行
        DataStream<String> result = s1.join(s2)
                .where(tp -> tp.f0)
                .equalTo(tp -> tp.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
                    /**
                     *  把窗口内关联上的数据逐条处理
                     * @param left 左表数据
                     * @param right 右表数据
                     * @return 处理之后返回
                     */
                    @Override
                    public String join(Tuple2<String, String> left, Tuple3<String, String, String> right) {
                        return left.f0 + "," + left.f1 + "," + right.f1 + "," + right.f2;
                    }
                });
        result.print();
        environment.execute();
    }
}
