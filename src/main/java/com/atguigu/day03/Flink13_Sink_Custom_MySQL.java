package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author JianJun
 * @create 2021/12/12 23:15
 */
public class Flink13_Sink_Custom_MySQL {
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

        //TODO 自定义Sink
        map.addSink(new MySinkFun());
        env.execute();
    }

    //    public static class MySinkFun implements SinkFunction<WaterSensor>{
    //使用富函数优化连接
    public static class MySinkFun extends RichSinkFunction<WaterSensor> {
        private Connection connection;
        private PreparedStatement pstm;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("创建连接。。。");
            //获取连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456");
            //获取语句预执行者
            pstm = connection.prepareStatement("insert into sensor values (?,?,?)");
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {

            //给占位符赋值
            pstm.setString(1, value.getId());
            pstm.setLong(2, value.getTs());
            pstm.setInt(3, value.getVc());

            pstm.execute();

            //当为自动提交时 不用手动提交
//            connection.commit();

        }

        @Override
        public void close() throws Exception {
            System.out.println("关闭连接。。。");
            //关闭相关资源和连接
            pstm.close();
            connection.close();
        }
    }
}
