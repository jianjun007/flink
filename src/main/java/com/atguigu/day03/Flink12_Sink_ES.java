package com.atguigu.day03;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;

/**
 * @author JianJun
 * @create 2021/12/12 22:17
 */
public class Flink12_Sink_ES {
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

        //TODO 将数据写入ES
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));
        httpHosts.add(new HttpHost("hadoop103", 9200));
        httpHosts.add(new HttpHost("hadoop104", 9200));

        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<WaterSensor>() {
            @Override
            public void process(WaterSensor waterSensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                //指定要插入的索引类型以及docId
                IndexRequest indexRequest = new IndexRequest("0726_flink", "_doc", "1001");
                String jsonString = JSONObject.toJSONString(waterSensor);
                //放入数据显示声明未JSON字符串
                IndexRequest source = indexRequest.source(jsonString, XContentType.JSON);
                //添加写入请求
                requestIndexer.add(indexRequest);
            }
        });
        //因为读的是无界流数据，ES中会默认将数据先缓存起来，如果要实现来一条写一条，这把这个参数设置为1. 注意：生产中不要这么设置
        waterSensorBuilder.setBulkFlushMaxActions(1);
        ElasticsearchSink<WaterSensor> elasticsearchSink = waterSensorBuilder.build();
        map.addSink(elasticsearchSink);
        env.execute();


    }
}
