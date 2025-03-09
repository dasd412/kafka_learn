package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerPartitionAssign {
    /*
    특정 파티션을 컨슈머에게 할당하는 건 일반적이진 않음.
     */
    public static final Logger logger = LoggerFactory.getLogger(ConsumerPartitionAssign.class);

    public static void main(String[] args) {
        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-pizza-assign-seek");
        //props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        TopicPartition topicPartition = new TopicPartition(topicName,0);
//        kafkaConsumer.subscribe(List.of(topicName));
        kafkaConsumer.assign(Arrays.asList(topicPartition));


        // 메인 스레드 참조 변수
        Thread mainThread = Thread.currentThread();

        //메인 스레드가 셧다운 될 때의 훅 추가. 별도의 스레드로 카프카 컨슈머에게 wakeup() 호출
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("main program starts to exit by calling wake up");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();//메인스레드가 완전히 작업을 끝낼 때까지 대기
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        pollCommitSync(kafkaConsumer);
    }

    private static void pollCommitSync(KafkaConsumer<String, String> kafkaConsumer) {
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key:{}, record value:{}, partition:{}, recordOffset:{}",
                            record.key(), record.value(), record.partition(), record.offset());
                }

                try {
                    if (consumerRecords.count() > 0) {
                        kafkaConsumer.commitSync();
                        logger.info("commit sync");
                    }
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }

}
