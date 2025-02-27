package com.example.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducerCustomPartitioner {
    public static final Logger logger= LoggerFactory.getLogger(PizzaProducerCustomPartitioner.class);

    public static void sendPizzaMessage(KafkaProducer<String,String>kafkaProducer,String topicName, int iterCount,int interIntervalMillis, int intervalMillis, int intervalCount, boolean sync){
        PizzaMessage pizzaMessage = new PizzaMessage();

        int iterSeq=0;

        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while(iterSeq!=iterCount){
            HashMap<String, String> pMessage=pizzaMessage.produce_msg(faker,random,iterSeq);

            ProducerRecord<String,String>producerRecord=new ProducerRecord<>(topicName,pMessage.get("key"),pMessage.get("message"));

            sendMessage(kafkaProducer,producerRecord,pMessage,sync);

            if((intervalCount>0)&&(iterSeq%intervalCount==0)){
                try {
                    logger.info("### intervalCount :"+intervalCount+"intervalMillis"+interIntervalMillis+"###");
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            if(interIntervalMillis>0){
                try {
                    logger.info("interIntervalMillis : "+interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            iterSeq++;
        }
    }

    public static void sendMessage(KafkaProducer<String,String>kafkaProducer,ProducerRecord<String,String>producerRecord,HashMap<String, String> pMessage,boolean sync){
        if(!sync){
            kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
                if (exception==null){
                    logger.info("async message : "+pMessage.get("key")+"partition"+recordMetadata.partition()+"offset : "+recordMetadata.offset());
                }else{
                    logger.error("exception error from broker "+exception.getMessage());
                }
            });
        }else{
            try {
                RecordMetadata recordMetadata=kafkaProducer.send(producerRecord).get();
                logger.info("\n #### record metadata received ####"+"partition"+recordMetadata.partition()+"\n"+"offset:"+recordMetadata.offset());
            } catch (InterruptedException | ExecutionException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        String topicName="pizza-topic-partitioner";

        // kafkaProducer configuration setting
        Properties props=new Properties();
        //bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //acks 세팅
        //props.setProperty(ProducerConfig.ACKS_CONFIG,"0");
        //batch 세팅
        //props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"32000");
        //props.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        // 파티셔너 세팅
        props.setProperty("custom.specialKey","P001");
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.example.kafka.CustomPartitioner");


        // kafkaProducer object creation
        KafkaProducer<String,String>kafkaProducer=new KafkaProducer<>(props);

        sendPizzaMessage(kafkaProducer,topicName,-1,100,0,0,true);
        kafkaProducer.close();
    }
}
