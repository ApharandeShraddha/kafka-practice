package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

    private static final String bootStrapServers = "127.0.0.1:9092";
    private static final String offset_config = "earliest";
    private static final String topic = "first_topic";

    public static void main(String[] args) {

        //create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset_config);

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 2);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        long offsetToReadFrom = 44L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int noOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;


        //poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar += 1;
                logger.info("Key:" + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                if (noOfMessagesToRead >= numberOfMessagesReadSoFar) {
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the application");
    }
}
