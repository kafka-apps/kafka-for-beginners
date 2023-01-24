package org.kafka.consumer.custom.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kafka.consumer.custom.deserializer.ItemDeserializer;
import org.kafka.consumer.custom.domain.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * created a topic = item
 * with replicas=3 , partitions=3 on terminal at /kafka_2.13-3.2.1/bin/
 *   	 ./kafka-topics.sh --create --topic item --bootstrap-server localhost:9092 --replication-factor 3 --partitions 3
 */
public class ItemConsumerApproach2 {

    private static final Logger logger = LoggerFactory.getLogger(ItemConsumerApproach2.class);

    private static final String topicName = "item";

    private KafkaConsumer<Integer, String> createConsumer() {
        Map<String, Object> consumerPropertiesMap = new HashMap<>();
        consumerPropertiesMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        consumerPropertiesMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerPropertiesMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerPropertiesMap.put(ConsumerConfig.GROUP_ID_CONFIG, "itemsGroupId");
        consumerPropertiesMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //consumerPropertiesMap.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5000);
       // consumerPropertiesMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);
        return new KafkaConsumer<Integer, String>(consumerPropertiesMap);
    }

    private void pollKafka(KafkaConsumer<Integer, String> kafkaConsumer) {
        ObjectMapper objectMapper = new ObjectMapper();
        kafkaConsumer.subscribe(List.of(topicName));
        Duration timeOutDuration = Duration.of(100, ChronoUnit.MILLIS);
        try {
            while(true) {
                ConsumerRecords<Integer, String> consumerRecords = kafkaConsumer.poll(timeOutDuration);
                consumerRecords.forEach(record -> {
                    logger.info("consumer record: key {} and value {} and partition {}",
                            record.key(), record.value(), record.partition());
                    try {
                        Item item = objectMapper.readValue(record.value(), Item.class);
                        logger.info("item consumed: {}", item);
                    } catch (JsonProcessingException e) {
                        logger.error("JsonProcessingException in pollKafka while deserializing {}", e);
                    }
                });
            }
        } catch(Exception e) {
            logger.error("exception in pollKafka {}", e);

        } finally {
            kafkaConsumer.close();
        }

    }

    public static void main(String[] args) {
        ItemConsumerApproach2 messageConsumerReplication = new ItemConsumerApproach2();
        KafkaConsumer<Integer, String> kafkaConsumer = messageConsumerReplication.createConsumer();
        messageConsumerReplication.pollKafka(kafkaConsumer);
    }

}
