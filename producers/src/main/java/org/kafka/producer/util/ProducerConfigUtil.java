package org.kafka.producer.util;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class ProducerConfigUtil {

    private ProducerConfigUtil() {}

    public static Map<String, Object> createProducerPropertiesMap() {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        propertiesMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertiesMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return propertiesMap;
    }

    public static KafkaProducer<String, String> createProducer(Map<String, Object> producerPropertiesMap) {
        return new KafkaProducer<String, String>(producerPropertiesMap);
    }

    public static void publishMessageSynchronously(KafkaProducer<String, String> kafkaProducer,
                                                   String topicName, String key, String value, Logger logger) {
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

    public static void publishMessageASync(KafkaProducer<String, String> kafkaProducer,
                                           String topicName, String key, String value, Logger logger) {
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
}
