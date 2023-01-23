package org.kafka.consumer.commitoffsets;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.kafka.consumer.listener.MessageRebalanceListener;
import org.kafka.consumer.util.ConsumerConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageConsumerReplicationSeek {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerReplicationSeek.class);

    private static final String topicName = "test-topic-api-1-replication";

    private KafkaConsumer<String, String> createConsumer() {
        Map<String, Object> consumerPropertiesMap = ConsumerConfigUtil.createConsumerPropertiesMap();
        consumerPropertiesMap.put(ConsumerConfig.GROUP_ID_CONFIG, "messageconsumer");
        //consumerPropertiesMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //consumerPropertiesMap.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5000);
        //consumerPropertiesMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);
        consumerPropertiesMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return ConsumerConfigUtil.createKafkaConsumer(consumerPropertiesMap);
    }

    private void pollKafka(KafkaConsumer<String, String> kafkaConsumer) {
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();

        kafkaConsumer.subscribe(List.of(topicName), new MessageRebalanceListener(kafkaConsumer));

        Duration timeOutDuration = Duration.of(100, ChronoUnit.MILLIS);
        try {
            while(true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(timeOutDuration);
                consumerRecords.forEach(record -> {
                    logger.info("consumer record: key {} , value {} , partition {} and offset {}",
                            record.key(), record.value(), record.partition(), record.offset());
                    offsetAndMetadataMap.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, null)); //always add +1 to the offset to consume the correct/next offset data
                });
                if(consumerRecords.count() > 0) {
                    //kafkaConsumer.commitSync(offsetAndMetadataMap); //commits the last record offset returned by the poll
                    writeOffsetsMapToPath(offsetAndMetadataMap);
                    logger.info("offset committed");
                }
            }
        } catch(CommitFailedException e){
            logger.error("commitfailedexception in pollKafka {}",e);
        }   catch(Exception e) {
            logger.error("exception in pollKafka {}", e);

        } finally {
            kafkaConsumer.close();
        }
    }

    private void writeOffsetsMapToPath(Map<TopicPartition, OffsetAndMetadata> offsetsMap) throws IOException {

        FileOutputStream fout = null;
        ObjectOutputStream oos = null;
        try {
            fout = new FileOutputStream(ConsumerConfigUtil.serialiaziedFilePath);
            oos = new ObjectOutputStream(fout);
            oos.writeObject(offsetsMap);
            logger.info("Offsets Written Successfully!");
        } catch (Exception ex) {
            logger.error("Exception Occurred while writing the file : " + ex);
        } finally {
            if(fout!=null)
                fout.close();
            if(oos!=null)
                oos.close();
        }
    }

    public static void main(String[] args) {
        MessageConsumerReplicationSeek messageConsumerReplication = new MessageConsumerReplicationSeek();
        KafkaConsumer<String, String> kafkaConsumer = messageConsumerReplication.createConsumer();
        messageConsumerReplication.pollKafka(kafkaConsumer);
    }

}
