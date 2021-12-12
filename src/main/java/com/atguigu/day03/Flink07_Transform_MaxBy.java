package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author JianJun
 * @create 2021/12/12 18:00
 */
public class Flink07_Transform_MaxBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9991);

        //解析输入的字符串,按照","切分,返回一个WaterSensor对象
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //按照id进行聚合
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        //max(vc)
        keyedStream.max("vc").print("max:");
        //maxBy("vc",true)
        keyedStream.maxBy("vc", true).print("maxBy,true:");
        //maxBy("vc",false)
        keyedStream.maxBy("vc", false).print("maxBy,false:");

        //执行流
        env.execute();

    }
}
