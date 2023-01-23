package org.kafka.consumer.listener;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.kafka.consumer.util.ConsumerConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class MessageConsumerReplicationRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerReplicationRebalanceListener.class);

    private static final String topicName = "test-topic-api-1-replication";

    private KafkaConsumer<String, String> createConsumer() {
        Map<String, Object> consumerPropertiesMap = ConsumerConfigUtil.createConsumerPropertiesMap();
        consumerPropertiesMap.put(ConsumerConfig.GROUP_ID_CONFIG, "messageconsumer");
        consumerPropertiesMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //consumerPropertiesMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //consumerPropertiesMap.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5000);
        //consumerPropertiesMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);
        return ConsumerConfigUtil.createKafkaConsumer(consumerPropertiesMap);
    }

    private void pollKafka(KafkaConsumer<String, String> kafkaConsumer) {
        kafkaConsumer.subscribe(List.of(topicName), new MessageRebalanceListener(kafkaConsumer)); //linking with the MessageRebalanceListener
        Duration timeOutDuration = Duration.of(100, ChronoUnit.MILLIS);
        try {
            while(true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(timeOutDuration);
                consumerRecords.forEach(record -> {
                    logger.info("consumer record: key {} and value {} and partition {}",
                            record.key(), record.value(), record.partition());
                });
                kafkaConsumer.commitSync();
            }
        } catch(Exception e) {
            logger.error("exception in pollKafka {}", e);

        } finally {
            kafkaConsumer.close();
        }

    }

    public static void main(String[] args) {
        MessageConsumerReplicationRebalanceListener messageConsumerReplication = new MessageConsumerReplicationRebalanceListener();
        KafkaConsumer<String, String> kafkaConsumer = messageConsumerReplication.createConsumer();
        messageConsumerReplication.pollKafka(kafkaConsumer);
    }

}
