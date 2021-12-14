package com.atguigu.day04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink10_TimeWindow_Tumbing_Sliding_Session {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 设置运行时模式
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);


        //将并行度设置为1
        env.setParallelism(1);

        //2.读取无界数据，从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9991);
//        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        //3.将数据按照空格切分
        SingleOutputStreamOperator<String> wordDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        //4.将单词组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDStream = wordDStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });

        //5.将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDStream.keyBy(0);

       /* //TODO 开启一个基于时间的滚动窗口，窗口大小为5s
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));*/

        /*//TODO 开启一个基于时间的滚动窗口,窗口大小为6s,滑动步长为3s
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(6), Time.seconds(3)));*/

        // TODO 开启一个基于时间的会话窗口,使用静态会话间隔为5s
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        // 对窗口中的数据进行累加计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = window.sum(1);

        window.process(new ProcessWindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>() {
                           @Override
                           public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                               String msg =
                                       "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                               + elements.spliterator().estimateSize() + "条数据 ";
                               out.collect(msg);
                           }
                       }
        ).print();
        sum.print();


        //8.执行
        env.execute();

    }
}