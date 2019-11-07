package com.helloyoung.utils;

import com.alibaba.fastjson.JSON;
import entitys.UrlInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Author ：YOUNG
 * @Date ：Created in 2019/11/7 下午2:09
 * @Description：
 * 发送消息到kafka
 */
@Slf4j
public class KafkaSender {
    public static final String kafkaTopic = "testjin";

    public static final String brokerAddress = "localhost:9092";

    private static Properties properties;

    private static void init(){
        properties = new Properties();
        properties.put("bootstrap.servers", brokerAddress);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    private static void sendUrlToKafka(){
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        UrlInfo urlInfo = new UrlInfo();
        long currentMills = System.currentTimeMillis();
        if (currentMills % 100 > 30) {
            urlInfo.setUrl("http://so.com/" + currentMills);
        } else {
            urlInfo.setUrl("http://baidu.com/" + currentMills);
        }

        // 确保发送的消息都是string类型
        String msgContent = JSON.toJSONString(urlInfo);
        ProducerRecord record = new ProducerRecord<String, String>(kafkaTopic, null, null, msgContent);
        producer.send(record);

        log.info("send msg:" + msgContent);
        producer.flush();

    }




    public static void main(String[] args) throws InterruptedException {
        init();

        while (true){
            //每三秒一次
            Thread.sleep(3000);
            //发送消息到kafka
            sendUrlToKafka();
        }

    }








}
