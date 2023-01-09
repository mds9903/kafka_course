package com.course.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithGracefullShutdown {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithGracefullShutdown.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("ConsumerDemo main started");

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "myGroup123_with_graceful_shutdown";
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

        // <---------- code for graceful shutdown

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("GRACEFUL-SHUTDOWN: detected a shutdown, exiting by calling consumer.wakeup()...");
                myConsumer.wakeup(); // will cause the "myConsumer.poll()" action to throw a wakeup exception

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });

        // code for graceful shutdown ----------->

        try {

            // consume the messages
            //--subscribe our consumer to the topics
            myConsumer.subscribe(Collections.singleton(topic));

            Duration waitTimeUntilReply = Duration.ofMillis(100); // in milliseconds

            //--pull for new data
            while (true) {
                logger.info("Polling...");
                // create a record to store data from messages
                ConsumerRecords<String, String> myRecords = myConsumer.poll(waitTimeUntilReply);

                for (ConsumerRecord<String, String> record : myRecords) {
                    logger.info("Record Key: " + record.key() + ", Record Value: " + record.value());
                    logger.info("Record Partition: " + record.partition() + ", Record Offset: " + record.offset());
                }
            }
        } catch (WakeupException e){
            logger.info("GRACEFUL SHUTDOWN: wakeup exception occurred");
            // ignored as it is expected
        }
        catch (Exception e) {
            logger.error("UNEXPECTED EXCEPTION OCCURRED; EXCEPTION INFO: "+e);
//            e.printStackTrace();

        }finally {
            myConsumer.close(); // also commit the offsets if needed
            logger.info("GRACEFUL SHUTDOWN: closing consumer");
        }


    }
}

