package org.kafka.learn;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class MessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    String topicName = "test-topic-api-1";

    KafkaProducer<String, String> kafkaProducer;

    public MessageProducer(Map<String, Object> producerPropertiesMap) {
        kafkaProducer = new KafkaProducer<String, String>(producerPropertiesMap);
    }

    private static Map<String, Object> createProducerPropertiesMap() {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        propertiesMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertiesMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return propertiesMap;
    }

    private void publishMessageSync(String key, String value) {
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

    private void publishMessageASync(String key, String value) {
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

    public static void main(String[] args) {
        MessageProducer messageProducer = new MessageProducer(createProducerPropertiesMap());
        //messageProducer.publishMessageSync(null, "sending message2 from api call");
        messageProducer.publishMessageASync(null, "sending message asynchronously from api call");

        //sometimes message is published after the main thread is ending due to async call.
        //adding 3 seconds sleep for main thread after the async method call
        try {
            logger.info("holding the main thread to sleep for 3 seconds");
            Thread.sleep(3000);
            logger.info("sleep for 3 seconds completed");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
