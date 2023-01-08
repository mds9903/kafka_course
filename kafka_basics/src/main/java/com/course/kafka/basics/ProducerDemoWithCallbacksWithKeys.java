package com.course.kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacksWithKeys {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacksWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("ProducerDemo main started");

        // create producer properties
        Properties properties = new Properties();

        // safer way using ProducerConfig
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> myProducer = new KafkaProducer<>(properties);

        String topic = "demo_topic_with_keys";

        for(int i=0;i<10;i++) {

            String message = "message"+i;
            String key = "id_"+i;

            // create a producer record
            ProducerRecord<String, String> myProducerRecord = new ProducerRecord<>(topic, message, key);

            // send data (write); Note: an async operation
            myProducer.send(myProducerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // gets called when a message is successfully completed and sent to kafka
                    // (or not if exception occurred)
                    if (exception == null) {
                        logger.info("received new meta data\n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Key: " + myProducerRecord.key() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset Info: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        logger.error("Error while producing: ", exception);
                    }
                }
            });
        }
        // flush and close the producer; Note: a sync operation
        myProducer.flush(); // only flush
        myProducer.close(); // does the flush and close

    }
}
