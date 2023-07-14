package com.zhyf.highlevel;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutput {
    public static void main(String[] args) throws Exception {
        // 1 创建一个入口环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.socketTextStream("172.17.195.93", 9999);

        OutputTag<Integer> oddTag = new OutputTag<Integer>("odd-tag") {};
        OutputTag<Integer> evenTag = new OutputTag<Integer>("even-tag") {};
        OutputTag<String> strTag = new OutputTag<String>("str-tag") {};
        SingleOutputStreamOperator<String> mainStream = streamSource.process(new ProcessFunction<String, String>() {

            // 給數據打標簽
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                try {
                    int i = Integer.parseInt(value);
                    if (i % 2 == 0) {
                        ctx.output(oddTag, i);
                    } else {
                        ctx.output(evenTag, i);
                    }
                } catch (NumberFormatException e) {
                    ctx.output(strTag, value);
                }
            }
        });
        DataStream<Integer> evenOutput = mainStream.getSideOutput(evenTag);
        DataStream<Integer> oddOutput = mainStream.getSideOutput(oddTag);
        DataStream<String> stringOutput = mainStream.getSideOutput(strTag);

        stringOutput.print("STR");
        oddOutput.print("ODD");
        evenOutput.print("EVEN");
        mainStream.print("MAIN");
        environment.execute();

    }
}
