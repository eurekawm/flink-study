package com.zhyf.test;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.checkerframework.checker.units.qual.K;

import java.util.Random;

/**
 * 流1 id event 3
 * 1,event01, 2
 * 2,event02, 3
 * <p>
 * 流2 id gender city
 * 1, male, cd
 * <p>
 * 1. 展开流1 id, event 随机数
 * 1, event01, 1
 * 1, event01, 2
 * 1, event01, 3
 * 2. 流1的数据关联流2
 * <p>
 * 3. 把关联不上的数据作为侧流输出
 * 4. 按照性别分组 取出随机数最大的那条 输出
 * 5. 侧流写入文件系统 主流写入mysql 幂等
 */


public class Test1 {
    public static void main(String[] args) {
        // 1 创建一个入口环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
        // 2 通过source算子映射数据源为一个dataStream
        // id gender, city
        DataStreamSource<String> streamSource1 = environment.socketTextStream("192.168.101.26", 9999);
        // id eventId, cnt
        DataStreamSource<String> streamSource2 = environment.socketTextStream("192.168.101.26", 8888);

        SingleOutputStreamOperator<UserInfo> userStream = streamSource1.map(s -> {
            String[] split = s.split(",");
            return UserInfo.builder().id(Integer.parseInt(split[0]))
                    .gender(split[1])
                    .city(split[2]).build();
        }).returns(new TypeHint<UserInfo>() {
        });


        SingleOutputStreamOperator<EventCount> eventStream = streamSource2.map(s -> {
            String[] split = s.split(",");
            return EventCount.builder().id(Integer.parseInt(split[0]))
                    .eventId(split[1])
                    .cnt(Integer.parseInt(split[2]))
                    .build();
        }).returns(new TypeHint<EventCount>() {
        });

        //1. 对流进展开
        SingleOutputStreamOperator<EventCount> flatMaped = eventStream.process(new ProcessFunction<EventCount, EventCount>() {
            @Override
            public void processElement(EventCount value, ProcessFunction<EventCount, EventCount>.Context ctx, Collector<EventCount> out) throws Exception {
                int cnt = value.getCnt();
                for (int i = 0; i < cnt; i++) {
                    out.collect(new EventCount(value.getId(), value.getEventId(), RandomUtils.nextInt(1, 99)));

                }
            }
        });
        //2. 关联需求 用广播流 因为用户信息是很少
        // 用户维度信息 key -> id value -> 用户维度信息
        MapStateDescriptor<Integer, UserInfo> stateDescriptor = new MapStateDescriptor<>("S", Integer.class, UserInfo.class);
        // 侧流输出tag
        OutputTag<EventCount> outputTag = new OutputTag<>("c", TypeInformation.of(EventCount.class));
        BroadcastStream<UserInfo> broadcast = userStream.broadcast(stateDescriptor);
        SingleOutputStreamOperator<EventUserInfo> joinedStream = flatMaped.connect(broadcast).process(new BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>() {

            /**
             * 主流处理
             */
            @Override
            public void processElement(EventCount value, BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>.ReadOnlyContext ctx, Collector<EventUserInfo> out) throws Exception {
                ReadOnlyBroadcastState<Integer, UserInfo> broadcastState = ctx.getBroadcastState(stateDescriptor);
                UserInfo userInfo = null;
                if (broadcastState != null && (userInfo = broadcastState.get(value.getId())) != null) {
                    // 关联成功的输出到主流
                    out.collect(new EventUserInfo(value.getId(), value.getEventId(), value.getCnt(), userInfo.getGender(), userInfo.getCity()));
                } else {
                    // 没关联上的输出到侧流
                    ctx.output(outputTag, value);
                };
            }

            /**
             * 广播流处理
             */
            @Override
            public void processBroadcastElement(UserInfo value, BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>.Context ctx, Collector<EventUserInfo> out) throws Exception {
                BroadcastState<Integer, UserInfo> broadcastState = ctx.getBroadcastState(stateDescriptor);
                broadcastState.put(value.getId(), value);
            }
        });
        // 按照性别分组 取出随机数最大的那条 输出
        SingleOutputStreamOperator<EventUserInfo> mainRes = joinedStream
                .keyBy(EventUserInfo::getGender)
                .maxBy("cnt");
        // 侧流写入文件系统 主流写入mysql 幂等

    }
}
