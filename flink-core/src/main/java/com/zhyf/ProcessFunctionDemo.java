package com.zhyf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流
 */
public class ProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.readTextFile("flink-core/src/main/resources/data/userinfo.txt");
        environment.setParallelism(1);

        // process是主流
        SingleOutputStreamOperator<String> process = streamSource.process(new ProcessFunction<String, String>() {
            /**
             *
             * @param in The input value. 输入的数据
             * @param ctx 上下文 可以提供侧输出的功能
             * @param out The collector for returning result values. 输出的数据
             * @throws Exception
             */
            @Override
            public void processElement(String in, ProcessFunction<String, String>.Context ctx, Collector<String> out) {
                if (in.length() > 200) {
                    ctx.output(new OutputTag<String>("moreThan", TypeInformation.of(String.class)), in);
                } else {
                    ctx.output(new OutputTag<String>("LessThan", TypeInformation.of(String.class)), in);
                }
                out.collect(in);
            }
        });

        // 获取lessThan流
        DataStream<String> lessThan = process.getSideOutput(new OutputTag<String>("moreThan", TypeInformation.of(String.class)));
        lessThan.print("lessThan");
//        process.print();

        environment.execute();
    }
}
