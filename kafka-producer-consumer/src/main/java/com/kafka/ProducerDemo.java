package com.kafka;

import com.sun.corba.se.internal.CosNaming.BootstrapServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    private static final String bootStrapServers = "127.0.0.1:9092";


    public static void main(String[] args) {


      //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("firsttopic","hello");

        //send data
        producer.send(record);

        //flush and close
        producer.flush();
        producer.close();

    }
}
