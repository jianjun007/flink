package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author JianJun
 * @create 2021/12/14 9:59
 */
public class Flink06_SideOutput {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9991);

        //3.将数据转换成JavaBean类型的数据
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(value -> {
            String[] split = value.split(",");
            return new WaterSensor(
                    split[0],
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2])
            );
        });


        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(value -> value.getId());

        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                if (value.getVc() > 5) {
                    ctx.output(new OutputTag<WaterSensor>("warn"){}, value);
                }
            }
        });


        process.print("主流");
        process.getSideOutput(new OutputTag<WaterSensor>("warn"){}).print("警告");


        env.execute();
    }
}
