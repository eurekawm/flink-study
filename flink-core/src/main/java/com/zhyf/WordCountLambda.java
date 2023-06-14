package com.zhyf;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountLambda {
    public static void main(String[] args) throws Exception {
        // 1 创建一个入口环境

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8081);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        // 2 通过source算子映射数据源为一个dataStream
        DataStreamSource<String> dataStreamSource = environment.socketTextStream("192.168.101.26", 9999);
        dataStreamSource
                .map(String::toUpperCase)
                .flatMap((String s, Collector<Tuple2<String, Integer>> out) -> {
                    String[] split = s.split("\\s+");
                    for (String word : split) {
                        out.collect(Tuple2.of(word, 1));
                    }})
                // returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>()));
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy( item -> item.f0)
                .sum("f1")
                .print("sink");
        environment.execute();
    }
}
