package com.wmy.hotItemsAnalysis.process;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URL;
import java.util.Properties;

/**
 * ClassName:KafkaProducerUtil
 * Package:com.wmy.hotItemsAnalysis.process
 *
 * @date:2021/6/24 20:30
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: 读取文件往Kafka中写入数据
 */
public class KafkaProducerUtil {
    public static void main(String[] args) throws Exception{
        writeToKafka("hotitems");
    }

    // 包装一个写入kafka的方法
    public static void writeToKafka(String topic) throws Exception{
        // kafka 配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "yaxin01:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 定义一个Kafka Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 用缓冲方式读取文本
        URL resource = HotItems.class.getResource("/UserBehavior.csv");
        BufferedReader bufferedReader = new BufferedReader(new FileReader(resource.getPath()));
        String line;
        while( (line = bufferedReader.readLine()) != null ){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, line);
            // 用producer发送数据
            kafkaProducer.send(producerRecord);
        }
        kafkaProducer.close();
    }
}
