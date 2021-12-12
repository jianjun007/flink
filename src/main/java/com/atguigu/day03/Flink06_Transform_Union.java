package com.atguigu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author JianJun
 * @create 2021/12/11 9:20
 */
public class Flink06_Transform_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        //从元素中获取数据
        DataStreamSource<String> strDStream = env.fromElements("a", "b", "c", "d","e");
        DataStreamSource<String> intDStream = env.fromElements("1", "2", "3", "4", "5");
        DataStreamSource<String> nameDStream = env.fromElements("赵", "钱", "孙", "李", "周");

        //TODO 使用Union连接两条流

        DataStream<String> union = strDStream.union(intDStream, nameDStream);
        union.print();


        union.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();


        env.execute();
    }
}
