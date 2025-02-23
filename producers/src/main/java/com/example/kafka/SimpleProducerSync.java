package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleProducerSync {
    public static final Logger logger= LoggerFactory.getLogger(SimpleProducerSync.class);

    public static void main(String[] args) {
        String topicName="simple-topic";

        // kafkaProducer configuration setting
        Properties props=new Properties();
        //bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // kafkaProducer object creation
        KafkaProducer<String,String>kafkaProducer= new KafkaProducer<>(props);

        //ProducerRecord should match key,value serializer class
        ProducerRecord<String,String>producerRecord=new ProducerRecord<>(topicName,"id-001","hello world");

        //kafkaProducer message send with sync
        try {
            RecordMetadata recordMetadata=kafkaProducer.send(producerRecord).get();
            logger.info("\n #### record metadata received ####"+"partition"+recordMetadata.partition()+"\n"+"offset:"+recordMetadata.offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            kafkaProducer.close();
        }
    }
}
