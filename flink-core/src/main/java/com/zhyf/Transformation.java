package com.zhyf;

import com.alibaba.fastjson2.JSONObject;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

@Slf4j
public class Transformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.readTextFile("flink-core/src/main/resources/data/userinfo.txt");
        environment.setParallelism(1);
        /**
         * map 算子
         */
        SingleOutputStreamOperator<UserInfo> map = streamSource.map(json -> JSONObject.parseObject(json, UserInfo.class));
        map.print();

//        /**
//         * filter算子
//         */
//        SingleOutputStreamOperator<UserInfo> filter = map.filter(item -> item.getFriends().size() >= 3);
//        filter.print();
//
//        /**
//         * flatMap算子
//         */
//        SingleOutputStreamOperator<FlatMapUserInfo> flatMap = filter.flatMap((UserInfo u, Collector<FlatMapUserInfo> out) -> {
//            List<FriendInfo> friends = u.getFriends();
//            for (FriendInfo friend : friends) {
//                FlatMapUserInfo flatMapUserInfo = new FlatMapUserInfo();
//                flatMapUserInfo.setGender(u.getGender());
//                flatMapUserInfo.setUid(u.getUid());
//                flatMapUserInfo.setName(u.getName());
//                flatMapUserInfo.setFid(friend.getFid());
//                flatMapUserInfo.setFname(friend.getFname());
//                out.collect(flatMapUserInfo);
//            }
//        }).returns(TypeInformation.of(FlatMapUserInfo.class));
//        log.info("flatMap+++++++++++++++++++++++++++++++++++++++++");
//        flatMap.print();
//
//        /**
//         * 分组算子 以性别分组 每个性别的数量
//         */
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = flatMap.map(user -> Tuple2.of(user.getGender(), 1))
//                .returns(new TypeHint<Tuple2<String, Integer>>() {
//                })
//                .keyBy(tp -> tp.f0)
//                .sum(1);
//        sum.print();
        /**
         * 求各性别里面 好友数最大值
         * max和maxBy都是滚动更新 不会把所有的数据都存起来 新的数据覆盖旧的数据 max 只更新要求的最大值的字段 而maxBy是更新所有字段
         */
//        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> max = map.map(item -> Tuple4.of(item.getUid(), item.getName(), item.getGender(), item.getFriends().size()))
//                .returns(new TypeHint<Tuple4<Integer, String, String, Integer>>() {})
//                .keyBy(tp -> tp.f2)
//                .maxBy(3);
//        max.print();

        /**
         *  需求 求各个性别中好友数量最大的那个，如果前后两个好友数量相同 输出结果中也需要将 uid / name等信息更新成后面那条数据的
         *  reduce方法的两个参数  in -> 此前的聚合值 out -> 本次的新数据 返回 更新后的聚合结果
         */
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> max = map.map(item -> Tuple4.of(item.getUid(), item.getName(), item.getGender(), item.getFriends().size()))
                .returns(new TypeHint<Tuple4<Integer, String, String, Integer>>() {
                });

        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> reduce = max.keyBy(tp -> tp.f2)
                .reduce(new ReduceFunction<Tuple4<Integer, String, String, Integer>>() {
                    @Override
                    public Tuple4<Integer, String, String, Integer> reduce(Tuple4<Integer, String, String, Integer> in, Tuple4<Integer, String, String, Integer> out) throws Exception {
                        if (in == null || out.f3 >= in.f3) {
                            return out;
                        } else {
                            return in;
                        }
                    }
                });
//        reduce.print();
        /**
         * 用reduce求各性别好友总和
         */
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> reduce1 = max.keyBy(tp -> tp.f2)
                .reduce(new ReduceFunction<Tuple4<Integer, String, String, Integer>>() {
                    @Override
                    public Tuple4<Integer, String, String, Integer> reduce(Tuple4<Integer, String, String, Integer> in, Tuple4<Integer, String, String, Integer> out) throws Exception {
                        out.setField(out.f3 + (in.f3 == null ? 0 : in.f3), 3);
                        return out;
                    }
                });
        reduce1.print();
        environment.execute();
    }
}

@Data
class UserInfo {
    private int uid;
    private String name;
    private String gender;
    private List<FriendInfo> friends;

}

@Data
class FriendInfo {
    private int fid;
    private String fname;
}

@Data
class FlatMapUserInfo {
    private int uid;
    private String name;
    private String gender;
    private int fid;
    private String fname;
}