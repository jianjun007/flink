package com.atguigu.day04;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author JianJun
 * @create 2021/12/13 13:05
 */
public class Flink08_Project_Order {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从文件中读取数据并转换为JavaBean
        SingleOutputStreamOperator<OrderEvent> orderDS = env.readTextFile("input/OrderLog.csv").map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
            }
        });

        //读取交易相关数据
        SingleOutputStreamOperator<TxEvent> tsDS = env.readTextFile("input/ReceiptLog.csv").map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(
                        split[0],
                        split[1],
                        Long.parseLong(split[2])
                );
            }
        });

        //3.通过connect将两条流连接起来
        ConnectedStreams<OrderEvent, TxEvent> connect = orderDS.connect(tsDS);

        //4.将相同的key聚合到一块
        ConnectedStreams<OrderEvent, TxEvent> orderEventTxEventConnectedStreams = connect.keyBy("txId", "txId");


        //5.实时对账
        orderEventTxEventConnectedStreams.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
            //创建一个Map集合用来存放订单表的数据
            HashMap<String, OrderEvent> orderMap = new HashMap<>();
            //创建一个Map集合用来存放交易表的数据
            HashMap<String, TxEvent> txMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                //1.去对方缓存中读取数据
                if (txMap.containsKey(value.getTxId())) {
                    //有能关联上的数据
                    out.collect("订单:" + value.getOrderId() + "对账成功");
                    //删除txMap中已经匹配上的数据
                    txMap.remove(value.getTxId());
                } else {
                    //没有能关联上的数据,将自己存入缓存
                    orderMap.put(value.getTxId(), value);
                }
            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                //1.去对方缓存中查数据
                if (orderMap.containsKey(value.getTxId())) {
                    //有能关联上的数据
                    out.collect("订单:" + orderMap.get(value.getTxId()).getOrderId() + "对账成功");
                    //删除orderMap中已经匹配上的数据
                    orderMap.remove(value.getTxId());
                } else {
                    //将没有能关联上的数据,将自己存入缓存
                    txMap.put(value.getTxId(), value);
                }
            }
        }).print();


        env.execute();

    }
}
