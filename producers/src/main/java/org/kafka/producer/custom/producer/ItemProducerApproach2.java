package org.kafka.producer.custom.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kafka.producer.custom.domain.Item;
import org.kafka.producer.custom.serializer.ItemSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * created a topic = item
 * with replicas=3 , partitions=3 on terminal at /kafka_2.13-3.2.1/bin/
 *   	 ./kafka-topics.sh --create --topic item --bootstrap-server localhost:9092 --replication-factor 3 --partitions 3
 */
public class ItemProducerApproach2 {

    private static final Logger logger = LoggerFactory.getLogger(ItemProducerApproach2.class);

    private static final String topicName = "item";

    private static KafkaProducer<Integer, String> createKafkaProducer() {
        Map<String, Object> producerPropertiesMap = createProducerPropertiesMap();
        return createItemProducer(producerPropertiesMap);
    }

    private static KafkaProducer<Integer, String> createItemProducer(Map<String, Object> producerPropertiesMap) {
        return new KafkaProducer<Integer, String>(producerPropertiesMap);
    }

    public static Map<String, Object> createProducerPropertiesMap() {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        propertiesMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        propertiesMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return propertiesMap;
    }


    private void publishMessageAsynchronously(KafkaProducer<Integer, String> kafkaProducer, Item item) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String itemValue = objectMapper.writeValueAsString(item);
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, item.getId(), itemValue);
        Callback callback = (metadata, exception) -> {
            if(Optional.ofNullable(exception).isPresent()) {
                logger.error("Exception occurred in callback {} ", exception.getMessage());
            } else {
                logger.info("Item {} sent successfully for the key {}", item, item.getId());
                logger.info("Published message Offset in callback is {} and partition : {}", metadata.offset(), metadata.partition());
            }
        };
        kafkaProducer.send(producerRecord, callback);
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
        ItemProducerApproach2 itemProducer = new ItemProducerApproach2();

        KafkaProducer<Integer, String> kafkaProducer = createKafkaProducer();

        Item item1 = new Item(3, "item3", 300.00);
        Item item2 = new Item(4, "item4", 400.00);

        //sending messages asynchronously
        Arrays.asList(item1, item2).forEach(
                element -> {
                    try {
                        itemProducer.publishMessageAsynchronously(kafkaProducer, element);
                    } catch (JsonProcessingException e) {
                        logger.error("JsonProcessingException in main {}", e);
                    }
                });
        itemProducer.addSleep(5000);



    }

}
