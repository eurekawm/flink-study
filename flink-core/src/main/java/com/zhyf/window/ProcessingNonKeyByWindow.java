package com.zhyf.window;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 窗口按照系统时间定期滚动 有数据就对数据进行运算
 */
public class ProcessingNonKeyByWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.socketTextStream("172.17.195.93", 9999);
        SingleOutputStreamOperator<Integer> map = streamSource.map(Integer::parseInt);
        // windowAll 划分nonKeyedWindow 方法参数是划分窗口的方式
        AllWindowedStream<Integer, TimeWindow> windowedStream = map.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);
        res.print();
        environment.execute();
    }
}
