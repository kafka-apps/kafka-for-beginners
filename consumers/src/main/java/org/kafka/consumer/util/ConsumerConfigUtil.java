package org.kafka.consumer.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

public class ConsumerConfigUtil {

    public static final String serialiaziedFilePath = "consumers/src/main/resources/offset.ser";

    private ConsumerConfigUtil() {}

    public static Map<String, Object> createConsumerPropertiesMap() {
        Map<String, Object> consumerPropertiesMap = new HashMap<>();
        consumerPropertiesMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        consumerPropertiesMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerPropertiesMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return consumerPropertiesMap;
    }

    public static KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> consumerPropertiesMap) {
        return new KafkaConsumer<String, String>(consumerPropertiesMap);
    }


}
