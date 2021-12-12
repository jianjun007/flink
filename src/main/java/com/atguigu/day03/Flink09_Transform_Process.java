package com.atguigu.day03;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author JianJun
 * @create 2021/12/12 18:58
 */
public class Flink09_Transform_Process {
    /**
     * 使用Process实现WordCount
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9991);

        //利用process实现flatMap
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapStream = streamSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = flatMapStream.keyBy(0);

        //保存上一次的累加结果
        HashMap<String, Integer> hashMap = new HashMap<>();
        //利用process实现sum功能
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2TupleKeyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                if (!hashMap.containsKey(value.f0)) {
                    //不存在，则将自己个数存进去
                    hashMap.put(value.f0, value.f1);
                } else {
                    //存在，则取出并重新赋值后存入
                    Integer integer = hashMap.get(value.f0);
                    integer += value.f1;
                    hashMap.put(value.f0, integer);
                }
                out.collect(Tuple2.of(value.f0,hashMap.get(value.f0)));
            }

        });
        sum.print();
        env.execute();
    }
}
