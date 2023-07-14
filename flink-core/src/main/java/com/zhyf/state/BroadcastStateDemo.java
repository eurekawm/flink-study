package com.zhyf.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStateDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 维度数据 insert,1,图书
        DataStreamSource<String> souce1 = environment.socketTextStream("172.17.195.93", 9999);
        // 行为数据
        DataStreamSource<String> source2 = environment.socketTextStream("172.17.195.93", 8888);

        // 维度数据进行广播
        MapStateDescriptor<Integer, String> stateDescriptor = new MapStateDescriptor<>("broadcast-state", Integer.class, String.class);

        BroadcastStream<Tuple3<String, Integer, String>> broadcastStream = souce1.map(item -> {
                    String[] split = item.split(",");
                    return Tuple3.of(split[0], Integer.parseInt(split[1]), split[2]);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, Integer, String>>() {
                }))
                .broadcast(stateDescriptor);

        SingleOutputStreamOperator<Tuple3<String, Integer, Double>> orderStream = source2.map(item -> {
                    String[] split = item.split(",");
                    return Tuple3.of(split[0], Integer.parseInt(split[1]), Double.parseDouble(split[2]));
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, Integer, Double>>() {
                }));

        orderStream.connect(broadcastStream).process(new BroadcastProcessFunction<Tuple3<String, Integer, Double>, Tuple3<String, Integer, String>, Tuple4<String, Integer, Double, String>>() {

            //处理事件流方法a
            @Override
            public void processElement(Tuple3<String, Integer, Double> value, BroadcastProcessFunction<Tuple3<String, Integer, Double>, Tuple3<String, Integer, String>, Tuple4<String, Integer, Double, String>>.ReadOnlyContext ctx, Collector<Tuple4<String, Integer, Double, String>> out) throws Exception {
                ReadOnlyBroadcastState<Integer, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                String name = broadcastState.get(value.f1);
                out.collect(Tuple4.of(value.f0, value.f1, value.f2, name));
            }

            // 处理维度数据流的方法（广播流）
            @Override
            public void processBroadcastElement(Tuple3<String, Integer, String> value, BroadcastProcessFunction<Tuple3<String, Integer, Double>, Tuple3<String, Integer, String>, Tuple4<String, Integer, Double, String>>.Context ctx, Collector<Tuple4<String, Integer, Double, String>> out) throws Exception {
                String type = value.f0;
                BroadcastState<Integer, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                if ("delete".equals(type)) {
                    broadcastState.remove(value.f1);
                } else {
                    broadcastState.put(value.f1, value.f2);
                }
            }
        });
    }

}
