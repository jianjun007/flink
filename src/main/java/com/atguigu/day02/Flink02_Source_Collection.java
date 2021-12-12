package com.atguigu.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author JianJun
 * @create 2021/12/10 22:58
 */
public class Flink02_Source_Collection {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //TODO 2.从集合中获取数据
        List<String> list = Arrays.asList("1", "2", "3", "4");
        DataStreamSource<String> streamSource = env.fromCollection(list);

        //TODO 3.从元素中获取数据
        DataStreamSource<String> streamSource1 = env.fromElements("1", "2", "3", "4");

//        streamSource.print();
        streamSource1.print();
        env.execute();


    }
}
