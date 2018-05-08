package com.xxr.flink.other;

import java.util.Properties;  
import java.util.Random;  
import org.apache.commons.lang3.RandomStringUtils;

import com.xxr.flink.JDBCTestBase;

import kafka.javaapi.producer.Producer;  
import kafka.producer.KeyedMessage;  
import kafka.producer.ProducerConfig;  
public class KafkaProduce {  
    public static void main(String[] args) throws InterruptedException {  
        String broker = JDBCTestBase.kafka_hosts;  
        System.out.println("broker:" + broker);  
        String topic = JDBCTestBase.kafka_topic;  
        int count = 30;  
        Random random=new Random();  
        Properties props = new Properties();  
        props.put("metadata.broker.list", broker);  
        props.put("serializer.class", "kafka.serializer.StringEncoder");  
        props.put("request.required.acks", "1");  
        ProducerConfig pConfig = new ProducerConfig(props);  
        Producer<String, String> producer = new Producer<String, String>(  
                pConfig);  
        for (int i = 0; i < count; ++i) {  
            String josn=random.nextInt(10)+":"+RandomStringUtils.randomAlphabetic(10)+":"+random.nextInt(5000);  
            producer.send(new KeyedMessage<String, String>(topic, josn));  
            Thread.sleep(1000);  
            System.out.println("第"+i+"条数据已经发送");  
        }  
    }  
}  