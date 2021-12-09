package com.atguigu.day01;

/**
 * @author JianJun
 * @create 2021/12/9 10:10
 */
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_YarnMode_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test1(env);
        test2(env);
        test3(env);
    }

    public static void test1(StreamExecutionEnvironment env) throws Exception {

        DataStreamSource<String> stringDataStreamSource = env.fromElements("22222");
        stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
        env.execute();
    }

    public static void test2(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> stringDataStreamSource = env.fromElements("22222");
        stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
        env.execute();
    }

    public static void test3(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("hadoop102", 9999);
        stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
        env.execute();
    }

}