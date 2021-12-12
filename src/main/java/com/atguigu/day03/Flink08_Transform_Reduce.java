package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author JianJun
 * @create 2021/12/12 18:38
 */
public class Flink08_Transform_Reduce {
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

        //keyBy(id)
        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(value -> value.getId());

        keyedStream.reduce(new ReduceFunction<WaterSensor>() {
            /**
             *
             * @param value1 上次聚合后的数据
             * @param value2 当前的数据
             * @return
             * @throws Exception
             */
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("reduce...");
                return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
            }
        }).print();

        env.execute();

    }
}