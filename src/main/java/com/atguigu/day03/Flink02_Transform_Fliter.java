package com.atguigu.day03;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author JianJun
 * @create 2021/12/11 9:20
 */
public class Flink02_Transform_Fliter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9991);

        //TODO 利用Fliter将奇数过滤掉
        streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return Integer.parseInt(value) % 2 == 0;
            }
        }).print();


        env.execute();
    }
}
