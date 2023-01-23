package org.kafka.consumer.listener;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kafka.consumer.util.ConsumerConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
       /*
        logger.info("applying seekToBeginning applying");
        kafkaConsumer.seekToBeginning(partitions);
        */

        /*
        logger.info("applying seekToEnd applying");
        kafkaConsumer.seekToEnd(partitions);
         */

        Map<TopicPartition, OffsetAndMetadata> offsetMap = readOffsetSerializationFile();
        logger.info("Offsetmap {}", offsetMap);
        if(offsetMap.size() > 0) {
            partitions.forEach(partition -> {
                kafkaConsumer.seek(partition, offsetMap.get(partition));
            });
        }
    }

    private static Map<TopicPartition, OffsetAndMetadata> readOffsetSerializationFile()  {
        Map<TopicPartition, OffsetAndMetadata> offsetsMapFromPath = new HashMap<>();
        FileInputStream fileInputStream = null;
        BufferedInputStream bufferedInputStream = null;
        ObjectInputStream objectInputStream = null;
        try {
            fileInputStream = new FileInputStream(ConsumerConfigUtil.serialiaziedFilePath);
            bufferedInputStream = new BufferedInputStream(fileInputStream);
            objectInputStream = new ObjectInputStream(bufferedInputStream);
            offsetsMapFromPath = (Map<TopicPartition, OffsetAndMetadata>) objectInputStream.readObject();
            logger.info("Offset Map read from the path is : {} ", offsetsMapFromPath);
        } catch (Exception e) {
            logger.error("Exception Occurred while reading the file : " + e);
        } finally {
            try{
                if (objectInputStream != null)
                    objectInputStream.close();
                if (fileInputStream != null)
                    fileInputStream.close();
                if (bufferedInputStream != null)
                    bufferedInputStream.close();
            }catch (Exception e){
                logger.error("Exception Occurred in closing the exception : " + e);
            }
        }
        return offsetsMapFromPath;
    }
}
