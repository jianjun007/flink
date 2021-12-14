package com.atguigu.day04;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import scala.Int;

/**
 * @author JianJun
 * @create 2021/12/13 11:20
 */
public class Flink02_Project_PV_Process {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件中获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //3.使用Process处理数据
        streamSource.process(new ProcessFunction<String, Integer>() {
            //声明一个累加器有用来保存数据
           private Integer count  = 0;
            @Override
            public void processElement(String value, Context ctx, Collector<Integer> out) throws Exception {
                //a.取出我们需要的字段
                String behavior = value.split(",")[3];

                //b.过滤出pv数据
                if (behavior.equals("pv")) {
                  count ++;
                  out.collect(count);
                }
            }
        }).print();

        env.execute();


    }
}
