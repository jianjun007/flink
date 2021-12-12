package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @author JianJun
 * @create 2021/12/11 1:14
 */
public class Flink04_Source_Custom {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.自定义Source
        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource())
                .setParallelism(2);

        streamSource.print();

        env.execute();
    }

    //    public static class MySource implements SourceFunction<WaterSensor>{
    //多并行度方式
    public static class MySource implements ParallelSourceFunction<WaterSensor> {

        private Boolean isRunning = true;
        private Random random = new Random();

        /**
         * 用来生成数据或者产生数据发送出去的方法
         *
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (isRunning) {
                ctx.collect(new WaterSensor("sensor" + random.nextInt(100), System.currentTimeMillis(), random.nextInt(1000)));
                Thread.sleep(1000);
            }

        }

        /**
         * 用来取消数据发送的
         * 由系统自己调用
         */
        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
