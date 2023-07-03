package com.zhyf.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class C01_FixDelayRestartStrategy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.socketTextStream("172.17.195.93", 9999);
        SingleOutputStreamOperator<Integer> map = streamSource.map(Integer::parseInt);
        // 参数1 最大重启次数 参数2 每次重启延迟的时间
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000));
        environment.execute();
    }
}
