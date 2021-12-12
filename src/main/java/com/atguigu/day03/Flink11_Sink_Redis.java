package com.atguigu.day03;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author JianJun
 * @create 2021/12/12 21:23
 */
public class Flink11_Sink_Redis {
    public static void main(String[] args) throws Exception {

        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口中获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9991);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .build();

        map.addSink(new RedisSink<>(jedisPoolConfig, new RedisMapper<WaterSensor>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                /*//additionalKey指的是Redis大Key
                return new RedisCommandDescription(RedisCommand.HSET,"0726");*/
                return new RedisCommandDescription(RedisCommand.SET);
            }

            /**
             * 当使用hash类型时，这个key指的是Hash中的小Key
             * 如果不是hash类型时，这个key指的是Redis的key
             * @param waterSensor
             * @return
             */
            @Override
            public String getKeyFromData(WaterSensor waterSensor) {
                return waterSensor.getId();
            }

            @Override
            public String getValueFromData(WaterSensor waterSensor) {
                return JSONObject.toJSONString(waterSensor);
            }
        }));

        env.execute();

    }
}
