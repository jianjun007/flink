package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

/**
 * @author JianJun
 * @create 2021/12/14 19:02
 */
public class Flink07_ProcessingTime_Timer {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9991);

        //3.将数据转换成JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(value -> {
            String[] split = value.split(",");
            return new WaterSensor(
                    split[0],
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2])
            );
        });

        //4.将相同key的数据聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //TODO 注册一个基于处理时间的定时器
                System.out.println("注册定时器:" + ctx.timerService().currentProcessingTime()/1000);
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);

                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                System.out.println("触发定时器:" + ctx.timerService().currentProcessingTime()/1000);
            }
        });


        process.print();

        env.execute();
    }
}
