package com.zhyf.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 故障率重启策略在故障发生之后重启job，每个时间间隔发生故障的次数超过指定的最大次数，
 * job将最终失败。并且还可以在连续的两次重启尝试之间配置一段固定延迟时间
 * 就是在这个时间窗口内 只能重启N次
 */
public class C02_FailureRateRestartStrategy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.socketTextStream("172.17.195.93", 9999);
        SingleOutputStreamOperator<Integer> map = streamSource.map(Integer::parseInt);
        // 参数1 最大的重启次数
        // 参数2 时间窗口的长度
        // 参数3 每一次重启延迟的时间
        // 这个时间窗口内[60s] 只能重启3次 每次重启延迟10s 如果在60s内重启超过了3次 重启失败
        environment.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(60), Time.seconds(10)));
        environment.execute();
    }
}
