package com.zhyf.state;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Enumeration;

import java.util.Random;

/**
 * 统计同一个游戏同一个分区充值金额、
 * 1. 数据倾斜问题 加盐
 * 2. 写入延迟问题 划分窗口批量聚合写
 */
public class GameTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(30);
        DataStreamSource<String> streamSource = environment.socketTextStream("172.17.195.93", 9999);

        /**
         * 游戏id 分组id 用户id 金额
         * g0001,z01,u001,300
         * g0001,z01,u001,300
         * g0002,z02,u001,100
         * g0001,z01,u001,300
         * g0001,z02,u002,300
         * g0002,z01,u001,300
         * g0001,z01,u001,300
         */

        streamSource.map(item -> {
                    // 游戏id 分区id 随机盐 充值金额
                    Random random = new Random();
                    int i = random.nextInt(8);
                    String[] split = item.split(",");
                    return Tuple4.of(split[0], split[1], i, Double.parseDouble(split[3]));
                }).returns(TypeInformation.of(new TypeHint<Tuple4<String, String, Integer, Double>>() {}))
                .keyBy(new KeySelector<Tuple4<String, String, Integer, Double>, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> getKey(Tuple4<String, String, Integer, Double> value) throws Exception {
                        return Tuple3.of(value.f0, value.f1, value.f2);
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(3)
                .map(item -> {
                    return Tuple3.of(item.f0, item.f1, item.f3);
                }).returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Double>>() {}))
                .keyBy(new KeySelector<Tuple3<String, String, Double>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Tuple3<String, String, Double> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1);
                    }
                })
                .sum(2)
                .print();
        environment.execute();

    }
}
