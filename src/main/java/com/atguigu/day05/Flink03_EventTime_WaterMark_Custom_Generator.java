package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author JianJun
 * @create 2021/12/14 9:59
 */
public class Flink03_EventTime_WaterMark_Custom_Generator {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(2000);

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

        //TODO 自定义WaterMark策略
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = map.assignTimestampsAndWatermarks(new WatermarkStrategy<WaterSensor>() {
                    @Override
                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new MyWatermarkGenerator(Duration.ofSeconds(3));
                    }
                }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                })
        );

        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(value -> value.getId());

        //TODO 开启一个基于事件时间的滚动窗口,窗口大小为5s
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        /*//TODO 开启一个基于时间的滑动窗口,窗口大小为6s,滑动步长3s
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(6), Time.seconds(3)));*/
       /* //TODO 开启一个基于时间的会话窗口,设置静态间隔为3s
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(3)));*/

        window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "当前key: " + key
                        + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd()/1000 + ") 一共有 "
                        + elements.spliterator().estimateSize() + "条数据 ";
                out.collect(msg);
            }
        }).print();

        env.execute();
    }

    public static class MyWatermarkGenerator implements WatermarkGenerator {

        /**
         * 目前为止最大的时间戳,生成WaterMark时间戳的依据
         */
        private long maxTimestamp;

        /**
         * 乱序程度
         */
        private  long outOfOrdernessMillis;

        /**
         * 构造方法
         * @param maxOutOfOrderness 乱序程度
         */
        public MyWatermarkGenerator(Duration maxOutOfOrderness) {
            checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
            checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");

            this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

            // start so that our lowest watermark would be Long.MIN_VALUE.
            this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
        }


        /**
         * 间歇性调用
         * @param event
         * @param eventTimestamp 当前的事件时间
         * @param output
         */
        @Override
        public void onEvent(Object event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
            System.out.println("当前最大时间戳:" + maxTimestamp);
        }

        /**
         * 周期性调用
         * @param output
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("周期性生成WaterMark" + (maxTimestamp - outOfOrdernessMillis -1));
            output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
        }
    }

}
