package com.zhyf;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class MyKafkaSource {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8081);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("tp01")
                .setGroupId("gp01")
                .setBootstrapServers("localhost:9092")
                // 设置开始偏移量
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 把source设置成有界流 读取到指定位置了就停止 常用于重跑一段历史数据 读的数据 [起始偏移量 指定位置]
//                .setBounded(OffsetsInitializer.committedOffsets());
                // 设置成无界流  但并不会一直读数据而是到达指定位置就停止 但程序不退出 场景：需要从kafka中读取到固定长度数据然后和其他的流联合
//                .setBounded(OffsetsInitializer.committedOffsets());
                // 开启自动提交机制 KafkaSource并不依赖它 宕机的时候会从flink中的状态机中获取offset
                .setProperty("auto.offset.commit", "true")
                .build();
//        environment.addSource();  接收的SourceFunction接口的实现类
        DataStreamSource<String> dataStreamSource = environment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk-source");// 接收实现了Source接口的实现类
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
