package com.atguigu.day04;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author JianJun
 * @create 2021/12/13 12:56
 */
public class Flink07_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从文件中读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/AdClickLog.csv");

        //TODO 业务逻辑最终需求(省份-广告id,次数)
        streamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] strings = value.split(",");
                AdsClickLog adsClickLog = new AdsClickLog(
                        Long.parseLong(strings[0]),
                        Long.parseLong(strings[1]),
                        strings[2],
                        strings[3],
                        Long.parseLong(strings[4])
                );
                return Tuple2.of(adsClickLog.getCity() + "-" + adsClickLog.getAdId(), 1L);
            }
        }).keyBy(0)
                .sum(1).print();
        env.execute();
    }
}
