package org.kafka.learn;

import org.apache.kafka.clients.producer.*;
import org.kafka.util.KafkaConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * sample producer class to send messages synchronously and asynchronously
 */
public class MessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    String topicName = "test-topic-api-1";

    KafkaProducer<String, String> kafkaProducer;

    public MessageProducer(Map<String, Object> producerPropertiesMap) {
        kafkaProducer = new KafkaProducer<String, String>(producerPropertiesMap);
    }

    public void close(){
        kafkaProducer.close();
    }

    public void publishMessageSynchronously(String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
           // System.out.println("partition " + recordMetadata.partition() + " , offset : " + recordMetadata.offset());
            logger.info("message {} sent successfully  for the key : {} ", value, key);
            logger.info("published message for the partition : {} , offset : {}", recordMetadata.partition(), recordMetadata.offset());
        } catch (InterruptedException e) {
            logger.error("exception occurred under publishMessageSync {}", e.getMessage());
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void publishMessageASync(String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
        Callback callback = (metadata, exception) -> {
            if(Optional.ofNullable(exception).isPresent()) {
                logger.error("Exception occurred in callback {} ", exception.getMessage());
            } else {
                logger.info("message {} sent successfully for the key {}", value, key);
                logger.info("Published message Offset in callback is {} and partition : {}", metadata.offset(), metadata.partition());
            }
        };
        kafkaProducer.send(producerRecord, callback);
    }

    /**
     *  key1,2,3,4 are distributed among 3 partitions.
     *  messages any 2 keys sent to same partition
     *  the partition number decided by default partitioner
     **/
    private void sendMultipleMessagesUsingKeysSynchronously() {
        publishMessageSynchronously("key1", "message1 of key1");
        publishMessageSynchronously("key1", "message2 of key1");

        publishMessageSynchronously("key2", "message1 of key2");

        publishMessageSynchronously("key3", "message1 of key3");

        publishMessageSynchronously("key1", "message3 of key1");

        publishMessageSynchronously("key3", "message2 of key3");

        publishMessageSynchronously("key2", "message2 of key2");
        publishMessageSynchronously("key2", "message4 of key2");
        publishMessageSynchronously("key2", "message3 of key2");

        publishMessageSynchronously("key4", "message1 of key4");
        publishMessageSynchronously("key4", "message4 of key4");
        publishMessageSynchronously("key4", "message2 of key4");


    }

    public static void main(String[] args) {
        MessageProducer messageProducer = new MessageProducer(KafkaConfigUtil.createProducerPropertiesMap());

        //sending messages synchronously
        //messageProducer.publishMessageSynchronously(null, "sending message2 from api call");

        messageProducer.sendMultipleMessagesUsingKeysSynchronously();

        //asynchronously sending messages
        /*
        messageProducer.publishMessageASync(null, "sending message asynchronously from api call");

        //sometimes message is published after the main thread is ending due to async call.
        //adding 3 seconds sleep for main thread after the async method call
        messageProducer.addSleep(3000);
        */
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


}
