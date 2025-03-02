package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerMTopicRebalance {
    public static final Logger logger= LoggerFactory.getLogger(ConsumerMTopicRebalance.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-assign");
        //props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of("topic-p3-t1","topic-p3-t2"));


        // 메인 스레드 참조 변수
        Thread mainThread=Thread.currentThread();

        //메인 스레드가 셧다운 될 때의 훅 추가. 별도의 스레드로 카프카 컨슈머에게 wakeup() 호출
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                logger.info("main program starts to exit by calling wake up");
                kafkaConsumer.wakeup();

                try{
                    mainThread.join();//메인스레드가 완전히 작업을 끝낼 때까지 대기
                }catch(InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        try{
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord record : consumerRecords) {
                    logger.info("topic:{}, record key:{}, record value:{}, partition:{}, recordOffset:{}",
                            record.topic(),record.key(), record.value(), record.partition(),record.offset());
                }
            }
        }catch (WakeupException e){
            logger.error("wakeup exception has been called");
        }finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }
}
