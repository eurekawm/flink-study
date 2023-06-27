package com.zhyf;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadCaseStream {
    public static void main(String[] args) throws Exception {
        // 1 创建一个入口环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
        // 2 通过source算子映射数据源为一个dataStream
        // id eventId
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
        // 案例背景：流1 是行为事件流 持续不断 一个人有多个事件发生
        //         流2 用户的维度信息 城市 年龄 等 一个人的数据只能来一次 来的事件不定 现在需要加工流1 把用户的维度信息填充好
        // 利用广播流来实现

        // 用户维度信息 key -> id value -> 用户维度信息
        // 字典数据流转换为广播流
        MapStateDescriptor<String, Tuple2<String, String>> userInfoState = new MapStateDescriptor<>("userInfoState", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }));
        BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String> broadcastProcessFunction = new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {

            BroadcastState<String, Tuple2<String, String>> broadcastState;

            /**
             *
             * @param value 主流中的数据 来一条就处理一条
             * @param ctx  上下文
             * @param out 处理结果收集器
             */
            @Override
            public void processElement(Tuple2<String, String> value, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 获取只读的广播状态
                ReadOnlyBroadcastState<String, Tuple2<String, String>> onlyBroadcastState = ctx.getBroadcastState(userInfoState);
                if (onlyBroadcastState != null) {
                    Tuple2<String, String> userInfo = onlyBroadcastState.get(value.f0);
                    // 拿出id 对应的用户信息
                    // 给主流添加 用户信息
                    out.collect(value.f0 + "," + value.f1 + "," + (userInfo != null ? userInfo.f0 : "null") + "," + (userInfo != null ? userInfo.f1 : "null"));
                } else {
                    out.collect(value.f0 + "," + value.f1 + "," + "null" + "," + "null");
                }
            }

            /**
             *
             * @param value 广播流里面的数据
             * @param ctx 上下问
             * @param out 结果收集器
             */
            @Override
            public void processBroadcastElement(Tuple3<String, String, String> value, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                // 广播流状态 一个被Flink管理的hashmap
                if (broadcastState == null) {
                    broadcastState = ctx.getBroadcastState(userInfoState);
                }
                // 把获得的这个广播流数据装入到广播状态 id, <age, city>
                broadcastState.put(value.f0, Tuple2.of(value.f1, value.f2));
            }
        };

        /**
         * s1 连接 s2生成的广播流 然后处理 sink
         */
        s1.connect(s2.broadcast(userInfoState)).process(broadcastProcessFunction).print();

        environment.execute();
    }
}
