package org.kafka.consumer.listener;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class MessageRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(MessageRebalanceListener.class);

    private KafkaConsumer<String, String> kafkaConsumer;

   public MessageRebalanceListener(KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("onPartitionsRevoked {}", partitions);
        kafkaConsumer.commitSync();
        logger.info("offsets committed");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("onPartitionsAssigned {}", partitions);
        logger.info("applying seekToBeginning");
        kafkaConsumer.seekToBeginning(partitions);
    }
}
