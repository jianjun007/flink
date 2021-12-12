package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author JianJun
 * @create 2021/12/12 23:18
 */
public class Flink14_Sink_JDBC {
    public static void main(String[] args) throws Exception {
//1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口中获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9991);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO 通过JDBC的方式写入Mysql

        SinkFunction<WaterSensor> jdbcSink = JdbcSink.<WaterSensor>sink(
                "insert into sensor values(?,?,?)",
                (pstm, value) -> {
                    pstm.setString(1, value.getId());
                    pstm.setLong(2, value.getTs());
                    pstm.setInt(3, value.getVc());
                },
                new JdbcExecutionOptions.Builder()
                        //数据来一条写一条
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test?useSSL=false")
                        .withUsername("root")
                        .withPassword("123456")
                        .withDriverName(Driver.class.getName())
                        .build()
        );


        map.addSink(jdbcSink);
        env.execute();
    }
}

