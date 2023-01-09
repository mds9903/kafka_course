package com.course.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("ConsumerDemo main started");

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "myGroup123";
        String topic = "demo_topic_with_keys";
        // create properties to config consumer
        Properties myProperties = new Properties();
        myProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        myProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        myProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        myProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        myProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create a consumer
        KafkaConsumer<String, String> myConsumer = new KafkaConsumer<>(myProperties);

        // consume the messages
        //--subscribe our consumer to the topics
        myConsumer.subscribe(Collections.singleton(topic));

        Duration waitTimeUntilReply = Duration.ofMillis(100); // in milliseconds

        //--pull for new data
        while(true){
            logger.info("Polling...");
            // create a record to store data from messages
            ConsumerRecords<String, String> myRecords = myConsumer.poll(waitTimeUntilReply);

            for(ConsumerRecord<String, String> record: myRecords){
                logger.info("Record Key: "+record.key()+", Record Value: "+record.value());
                logger.info("Record Partition: "+record.partition()+", Record Offset: "+record.offset());
            }
        }

    }
}

