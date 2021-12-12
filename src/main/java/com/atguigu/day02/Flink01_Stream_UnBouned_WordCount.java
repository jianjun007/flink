package com.atguigu.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author JianJun
 * @create 2021/12/10 11:55
 */
public class Flink01_Stream_UnBouned_WordCount {
    public static void main(String[] args) throws Exception {
        //1.获取执行流的包含local Web的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());

        //env并行度
        env.setParallelism(1);

        //2.读取无界数据流
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9991);

        //3.line->[word,word,...]
        SingleOutputStreamOperator<String> wordToOneDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).slotSharingGroup("group2").setParallelism(2);

        //4.[word,word,...]->(word,1)
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneDStream = wordToOneDStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, 1L);
            }
        }).slotSharingGroup("group1");

        //5.按相同的Key进行聚合
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordAndOneDStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });

        //6.对单词进行累加
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.sum(1);

        result.print();
        env.execute();
    }
}
