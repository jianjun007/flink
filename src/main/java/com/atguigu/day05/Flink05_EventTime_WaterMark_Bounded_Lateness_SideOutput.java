package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
public class Flink05_EventTime_WaterMark_Bounded_Lateness_SideOutput {
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

        //TODO 设置固定延迟的WaterMark,乱序时间设置为3s
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = map
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {

                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000;
                            }
                        }));

        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(value -> value.getId());

        //TODO 开启一个基于事件时间的滚动窗口,窗口大小为5s
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //TODO 设置允许迟到时间为3s
                .allowedLateness(Time.seconds(3))
                //TODO 将迟到的数据放到侧输出流中
                .sideOutputLateData(new OutputTag<WaterSensor>("late") {
                });


        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "当前key: " + key
                        + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                        + elements.spliterator().estimateSize() + "条数据 ";
                out.collect(msg);
            }
        });

        process.print("主流");

        //TODO 打印侧输出流的数据
        process.getSideOutput(new OutputTag<WaterSensor>("late"){}).print("侧输出流");

        env.execute();
    }
}
