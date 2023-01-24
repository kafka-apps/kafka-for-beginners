package org.kafka.consumer.custom.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.kafka.consumer.custom.domain.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ItemDeserializer implements Deserializer<Item> {

    private static final Logger logger = LoggerFactory.getLogger(ItemDeserializer.class);

    @Override
    public Item deserialize(String topic, byte[] data) {
        logger.info("deserializing the item");
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(data, Item.class);
        } catch (IOException e) {
            logger.error("JsonProcessingException in deserializing item {} , exception {} ", data, e);
            return null;
        }
    }
}
