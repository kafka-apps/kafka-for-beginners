package org.kafka.producer.custom.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.kafka.producer.custom.domain.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ItemSerializer implements Serializer<Item> {

    private static final Logger logger = LoggerFactory.getLogger(ItemSerializer.class);

    @Override
    public byte[] serialize(String topic, Item item) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            logger.info("serializing the item");
            return objectMapper.writeValueAsBytes(item);
        } catch (JsonProcessingException e) {
            logger.error("JsonProcessingException in serializing item , exception {} ", e);
            return null;
        }
    }
}
