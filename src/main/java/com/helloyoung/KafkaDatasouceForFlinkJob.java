package com.helloyoung;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * kafka作为数据源，消费kafka中的消息
 *
 * @Author ：YOUNG
 * @Date ：Created in 2019/11/7 下午2:01
 * @Description：
 */
public class KafkaDatasouceForFlinkJob {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("zookeeper.connect","localhost:2181");
        properties.put("group.id","metric-group");
        properties.put("auto.offset.reset","latest");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");


        DataStreamSource<String> dataStreamSource = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        "testjin",
                        new SimpleStringSchema(),
                        properties
                )
        ).setParallelism(1);


//        dataStreamSource.print();
        dataStreamSource.addSink(new PrintSinkFunction<>());

        env.execute("Flink add kafka data source");






    }
}
