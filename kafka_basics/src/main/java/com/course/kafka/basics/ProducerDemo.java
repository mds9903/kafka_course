package com.course.kafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        logger.info("ProducerDemo main started");

        // create producer properties
        Properties properties = new Properties();
        // hard coding the properties; can cause issues due to typos
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
//        properties.setProperty("key.serializer", "");
//        properties.setProperty("value.serializer", "");

        // safer way using ProducerConfig
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> myProducer = new KafkaProducer<>(properties);

        // create a producer record
        ProducerRecord<String, String> myProducerRecord = new ProducerRecord<>(
                "topic_demoJava", "hello world");

        // send data (write); Note: an async operation
        myProducer.send(myProducerRecord);

        // flush and close the producer; Note: a sync operation
        myProducer.flush(); // only flush
        myProducer.close(); // does the flush and close

    }
}
