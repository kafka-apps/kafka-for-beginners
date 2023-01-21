package org.kafka.learn;

import org.apache.kafka.clients.producer.*;
import org.kafka.util.KafkaConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * created a topic test-topic-api-1-replication
 * with replicas=3 , partitions=3 on terminal at /kafka_2.13-3.2.1/bin/
 *   	 ./kafka-topics.sh --create --topic test-topic-api-1-replication --bootstrap-server localhost:9092 --replication-factor 3 --partitions 3
 *   	 setting min.insync.replicas=2 for the topic
 *   	./kafka-configs.sh --alter --bootstrap-server localhost:9092 --entity-type topics --entity-name test-topic-api-1-replication --add-config min.insync.replicas=2
 *   start kafka console producer
 *   ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic-api-1-replication --from-beginning
 */
public class MessageProducerReplication {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducerReplication.class);

    private static String topicName = "test-topic-api-1-replication";

    public static String commandLineStartLogo(){

        return " __      _____________.____   _________  ________      _____  ___________                       \n" +
                "/  \\    /  \\_   _____/|    |  \\_   ___ \\ \\_____  \\    /     \\ \\_   _____/                       \n" +
                "\\   \\/\\/   /|    __)_ |    |  /    \\  \\/  /   |   \\  /  \\ /  \\ |    __)_                        \n" +
                " \\        / |        \\|    |__\\     \\____/    |    \\/    Y    \\|        \\                       \n" +
                "  \\__/\\  / /_______  /|_______ \\______  /\\_______  /\\____|__  /_______  /                       \n" +
                "       \\/          \\/         \\/      \\/         \\/         \\/        \\/                        \n" +
                "  __                                                                                            \n" +
                "_/  |_  ____                                                                                    \n" +
                "\\   __\\/  _ \\                                                                                   \n" +
                " |  | (  <_> )                                                                                  \n" +
                " |__|  \\____/                                                                                   \n" +
                "                                                                                                \n" +
                "_________                                           .___.____    .__                            \n" +
                "\\_   ___ \\  ____   _____   _____ _____    ____    __| _/|    |   |__| ____   ____               \n" +
                "/    \\  \\/ /  _ \\ /     \\ /     \\\\__  \\  /    \\  / __ | |    |   |  |/    \\_/ __ \\              \n" +
                "\\     \\___(  <_> )  Y Y  \\  Y Y  \\/ __ \\|   |  \\/ /_/ | |    |___|  |   |  \\  ___/              \n" +
                " \\______  /\\____/|__|_|  /__|_|  (____  /___|  /\\____ | |_______ \\__|___|  /\\___  >             \n" +
                "        \\/             \\/      \\/     \\/     \\/      \\/         \\/       \\/     \\/              \n" +
                " ____  __.       _____ __             __________                   .___                         \n" +
                "|    |/ _|____ _/ ____\\  | _______    \\______   \\_______  ____   __| _/_ __   ____  ___________ \n" +
                "|      < \\__  \\\\   __\\|  |/ /\\__  \\    |     ___/\\_  __ \\/  _ \\ / __ |  |  \\_/ ___\\/ __ \\_  __ \\\n" +
                "|    |  \\ / __ \\|  |  |    <  / __ \\_  |    |     |  | \\(  <_> ) /_/ |  |  /\\  \\__\\  ___/|  | \\/\n" +
                "|____|__ (____  /__|  |__|_ \\(____  /  |____|     |__|   \\____/\\____ |____/  \\___  >___  >__|   \n" +
                "        \\/    \\/           \\/     \\/                                \\/           \\/    \\/       ";
    }

    public static String BYE(){

        return "_______________.___.___________\n" +
                "\\______   \\__  |   |\\_   _____/\n" +
                " |    |  _//   |   | |    __)_ \n" +
                " |    |   \\\\____   | |        \\\n" +
                " |______  // ______|/_______  /\n" +
                "        \\/ \\/               \\/ ";
    }

    public static void launchCommandLine(){
        boolean cliUp = true;
        while (cliUp){
            Scanner scanner = new Scanner(System.in);
            userOptions();
            String option = scanner.next();
            logger.info("Selected Option is : {} ", option);
            switch (option) {
                case "1":
                    acceptMessageFromUser(option);
                    break;
                case "2":
                    cliUp = false;
                    break;
                default:
                    break;

            }
        }
    }

    public static void userOptions(){
        List<String> userInputList = new ArrayList<>();
        userInputList.add("1: Kafka Producer to produce messages ");
        userInputList.add("2: Exit");
        System.out.println("Please select one of the below options:");
        for(String userInput: userInputList ){
            System.out.println(userInput);
        }
    }
    public static KafkaProducer<String, String> createProducer(){
        Map<String, Object> producerProps = KafkaConfigUtil.createProducerPropertiesMap();
        return  KafkaConfigUtil.createProducer(producerProps);
    }

    public static void publishMessage(String input){
        Map<String, Object> producerPropertiesMap = KafkaConfigUtil.createProducerPropertiesMap();
        producerPropertiesMap.put(ProducerConfig.ACKS_CONFIG, "all");
        producerPropertiesMap.put(ProducerConfig.RETRIES_CONFIG, 10);
        producerPropertiesMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 3000);

        KafkaProducer<String, String> kafkaProducer = KafkaConfigUtil.createProducer(producerPropertiesMap);
        StringTokenizer stringTokenizer = new StringTokenizer(input, "-");
        Integer noOfTokens = stringTokenizer.countTokens();
        switch (noOfTokens){
            case 1:
                KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName,
                        null, stringTokenizer.nextToken(), logger);
                break;
            case 2:
                KafkaConfigUtil.publishMessageSynchronously(kafkaProducer, topicName,
                        stringTokenizer.nextToken(),stringTokenizer.nextToken(), logger);
                break;
            default:
                break;
        }
    }

    public static void acceptMessageFromUser(String option){
        Scanner scanner = new Scanner(System.in);
        boolean flag= true;
        while (flag){
            System.out.println("Please Enter a Message to produce to Kafka: type 00 to go back to the previous menu");
            String input = scanner.nextLine();
            logger.info("Entered message is {}", input);
            if(input.equals("00")) {
                flag = false;
            }else {
                KafkaProducer<String, String> kafkaProducer = createProducer();
                publishMessage(input);
                kafkaProducer.close();
            }
        }
        logger.info("Exiting from Option : " + option);
    }

    public static void main(String[] args) {

        System.out.println(commandLineStartLogo());
        launchCommandLine();
        System.out.println(BYE());

    }


}
