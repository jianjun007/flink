package com.atguigu.day04;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @author JianJun
 * @create 2021/12/13 11:28
 */
public class Flink03_Project_UV {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行换进
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从文件中读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //TODO 业务逻辑

        streamSource.process(new ProcessFunction<String, Integer>() {
            //创建set集合用来去重
            private HashSet<Long> hashSet = new HashSet<>();
            //声明一个累加器用来保存数据
            private Integer count = 0;

            @Override
            public void processElement(String value, Context ctx, Collector<Integer> out) throws Exception {
                //a.取出userId和behavior字段
                long userId = Long.parseLong(value.split(",")[0]);
                String behavior = value.split(",")[3];
                Tuple2<String, Long> tuple2 = new Tuple2<>(behavior, userId);
                //b.过滤出pv数据
                if (tuple2.f0.equals("pv") && !hashSet.contains(tuple2.f1)) {
                    hashSet.add(tuple2.f1);
                    count++;
                    out.collect(count);
                }
            }
        }).print();

        env.execute();
    }
}
