package org.kafka.learn;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
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

    public static void main(String[] args) {
        MessageProducer messageProducer = new MessageProducer(createProducerPropertiesMap());
        messageProducer.publishMessageSync(null, "sending message2 from api call");
    }
}
