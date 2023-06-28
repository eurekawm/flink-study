package com.zhyf.window;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 不keyBy直接划分
 */
public class CountWindowOn {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.socketTextStream("172.17.195.93", 9999);

        SingleOutputStreamOperator<Integer> map = streamSource.map(Integer::parseInt);
        // 按照条数划分窗口
        // 整个窗口最多保存10个数据 每5条滑动一次
        AllWindowedStream<Integer, GlobalWindow> windowedStream = map.countWindowAll(10, 5);
        // 对窗口内的数据进行计算
        SingleOutputStreamOperator<Integer> sum = windowedStream.sum(0);
        sum.print();
        environment.execute();
    }
}
