package com.helloyoung;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * Flink消费维基百科日志数据写入kafka Demo
 *
 * @author: YOUNG
 * @Date: 2019-11-06
 * @Description:
 */
public class WikipediaAnalysis {

    public static void main(String[] args) throws Exception {

        //1.获取环境信息
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.为环境信息添加WikipediaEditsSource源
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        //3.根据事件中的用户名为key来区分数据流
        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
                .keyBy(new KeySelector<WikipediaEditEvent, String>() {
                    @Override
                    public String getKey(WikipediaEditEvent event) {
                        return event.getUser();
                    }
                });

        DataStream<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(5))
                .aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return new Tuple2<>("", 0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(WikipediaEditEvent value, Tuple2<String, Long> accumulator) {
                        accumulator.f0 = value.getUser();
                        accumulator.f1 += value.getByteDiff();
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                        return new Tuple2<>(a.f0, a.f1 + b.f1);
                    }
                });

        result.map(new MapFunction<Tuple2<String, Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> tuple) throws Exception {
                return tuple.toString();
            }
        }).addSink(new FlinkKafkaProducer011<>("localhost:9092", "wiki-result", new SimpleStringSchema()));

        see.execute();
    }
}