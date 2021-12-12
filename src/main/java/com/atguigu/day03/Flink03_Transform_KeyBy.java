package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author JianJun
 * @create 2021/12/11 9:20
 */
public class Flink03_Transform_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9991);

        //TODO 利用Map将字符串拆分,new WaterSendor对象
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        }).setParallelism(2);

        //TODO keyBy
        //方式一:使用 KeySelector(推荐)
        /*KeyedStream<WaterSensor, String> keyedStream = map.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });*/

        //方式二:指定字段名字
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        map.print("原始分区:").setParallelism(2);
        keyedStream.print("keyBy:");


        env.execute();
    }
}
