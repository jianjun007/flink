package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author JianJun
 * @create 2021/12/8 11:48
 */
public class Flink03_Stream_UnBounded_WordCount {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取无界数据流
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9991);
        //3.将数据按照空格切分
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        //4.将相同的单词聚合到一起
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordToOneDStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            /**
             * 指定哪个字段作为进行聚合
             * @param value
             * @return
             * @throws Exception
             */
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });

        //5. 对单个单词个数进行累加
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.sum(1);

        result.print();
        env.execute();

    }
}
