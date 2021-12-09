package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author JianJun
 * @create 2021/12/8 10:27
 */
public class Flink02_Stream_Bounded_WordCount {
    public static void main(String[] args) throws Exception {
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //将并行度设置为1
        env.setParallelism(1);

        //2.读取文件
        DataStreamSource<String> lineDSS = env.readTextFile("input/words.txt");
        //3.转换数据格式
        //先将数据按照空格打散
        SingleOutputStreamOperator<String> wordDStream = lineDSS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                //按照空格切分
                String[] words = s.split(" ");
                //遍历出数组中的每一个单词
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        //4.将打散的单词组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDStream = wordDStream.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2 map(String s) throws Exception {
                return Tuple2.of(s, 1);

            }
        });

        //5.将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDStream.keyBy(0);
        //6.做累加计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //7.打印结果
        result.print();

        env.execute();
    }
}
