package org.producer.learn;

import org.apache.kafka.clients.producer.*;
import org.producer.util.KafkaConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * sample producer class to send messages synchronously and asynchronously
 */
public class MessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    String topicName = "test-topic-api-1";

    private static KafkaProducer<String, String> createKafkaProducer() {
        Map<String, Object> producerPropertiesMap = KafkaConfigUtil.createProducerPropertiesMap();
        return KafkaConfigUtil.createProducer(producerPropertiesMap);
    }


    private void publishMessageSynchronously(KafkaProducer<String, String> kafkaProducer, String topicName, String key, String value, Logger logger) {
        KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName, key, value, logger);
    }

    /**
     *  key1,2,3,4 are distributed among 3 partitions.
     *  messages any 2 keys sent to same partition
     *  the partition number decided by default partitioner
     **/
    private void sendMultipleMessagesUsingKeysSynchronously(KafkaProducer<String, String> kafkaProducer) {

        KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName, "key1", "message1 of key1", logger);
        KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName, "key1", "message2 of key1", logger);

        KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName, "key2", "message1 of key2", logger);

        KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName, "key3", "message1 of key3", logger);

        KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName, "key1", "message3 of key1", logger);

        KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName, "key3", "message2 of key3", logger);

        KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName, "key2", "message2 of key2", logger);
        KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName, "key2", "message4 of key2", logger);
        KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName, "key2", "message3 of key2", logger);

        KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName, "key4", "message1 of key4", logger);
        KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName, "key4", "message4 of key4", logger);
        KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName, "key4", "message2 of key4", logger);
    }

    private void publishMessageASync(KafkaProducer<String, String> kafkaProducer, String topicName, String key, String value, Logger logger) {
        KafkaConfigUtil.publishMessageASync(kafkaProducer, topicName, key, value, logger);
    }

    private void addSleep(int i) {
        try {
            logger.info("holding the main thread to sleep for 3 seconds");
            Thread.sleep(3000);
            logger.info("sleep for 3 seconds completed");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        MessageProducer messageProducer = new MessageProducer();

        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        //sending messages synchronously
        messageProducer.publishMessageSynchronously(kafkaProducer, messageProducer.topicName,null, "sending message2 from api call", logger);

//        messageProducer.sendMultipleMessagesUsingKeysSynchronously(kafkaProducer);

        //asynchronously sending messages
/*
        messageProducer.publishMessageASync(kafkaProducer, messageProducer.topicName, null, "sending message asynchronously from api call", logger);

        //sometimes message is published after the main thread is ending due to async call.
        //adding 3 seconds sleep for main thread after the async method call
        messageProducer.addSleep(3000);
*/
    }

}
