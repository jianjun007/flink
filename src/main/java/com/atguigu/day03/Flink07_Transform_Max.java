package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author JianJun
 * @create 2021/12/12 15:46
 */
public class Flink07_Transform_Max {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5);
        KeyedStream<Integer, String> kbStream = stream.keyBy(ele -> ele % 2 == 0 ? "奇数" : "偶数");
        kbStream.sum(0).print("sum");
        kbStream.max(0).print("max");
        kbStream.min(0).print("min");

        env.execute();
    }
}
