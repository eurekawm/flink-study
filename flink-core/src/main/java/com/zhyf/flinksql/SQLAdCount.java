package com.zhyf.flinksql;

import com.google.errorprone.annotations.Var;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * FlinkSQL实现广告点击的pv uv
 */
public class SQLAdCount {
    public static void main(String[] args) throws Exception {
        // 1 创建一个入口环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment SQLEnv = StreamTableEnvironment.create(environment);

        DataStreamSource<String> socketSource = environment.socketTextStream("172.17.195.93", 9999);
        /**
         * 用户id, 事件, 广告id
         * u01,view,a01
         * u02,click,a02
         */
        SingleOutputStreamOperator<Tuple3<String, String, String>> mapedStream = socketSource.map(item -> {
            String[] split = item.split(",");
            return Tuple3.of(split[0], split[1], split[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {});

        SQLEnv.createTemporaryView("v_data", mapedStream);

        //"select f0 word, sum(f1) from v_data group by f0
        TableResult tableResult = SQLEnv.executeSql("select f2 aid, f1 eid, count(*) counts, count(distinct(f0)) dis_counts from v_data group by f1, f2");

        tableResult.print();

        environment.execute();

    }
}
