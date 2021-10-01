package com.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    private static final String bootStrapServers = "127.0.0.1:9092";

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            //create a producer record

            String topic = "first_topic";
            String value = "hello World!" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key);

            //send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (Objects.isNull(e)) {
                        //record was successfully sent.
                        logger.info("Received new metadata:  \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing: " + e);
                    }
                }
            }).get(); // As asynchronous call this is for testing purpose , never do this on production
        }

        //flush and close
        producer.flush();
        producer.close();

    }
}
