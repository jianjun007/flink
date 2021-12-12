package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
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
public class Flink05_Transform_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        //从元素中获取数据
        DataStreamSource<String> stringDataStreamSource = env.fromElements("a", "b", "c", "e");

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);

        //TODO 使用Connect连接两条流
        ConnectedStreams<String, Integer> connect = stringDataStreamSource.connect(integerDataStreamSource);
        
        connect.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String value) throws Exception {
                return "Str:" + value;
            }

            @Override
            public String map2(Integer value) throws Exception {
                return "Int:" + value;
            }
        }).print();


        env.execute();
    }
}
