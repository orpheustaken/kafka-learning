import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka ConsumerDemo Class");

        String groupId = "java-app";
        String topic = "demo.java.topic";

        // Create Consumer Properties
        // Connect to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // Set Consumer Properties
        // To deserialize byte messages back to the actual data
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        // Possible values for this property are:
        //  none: if don't have existing consumer group, then fail
        //  earliest: starts reading from the beginning of the topic
        //  latest: reads only new messages
        properties.setProperty("auto.offset.reset", "earliest");

        // Create a Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to a topic
        // Using Arrays.asList it's possible to subscribe to multiple topics
        consumer.subscribe(Arrays.asList(topic));

        // Poll for data
        while (true) {
            log.info("Polling data from Kafka Topic " + topic);

            // Poll data and have a timeout of 5 seconds if there's nothing
            // Returns a collection (data structure for group of objects, similar to Arrays) of records
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

            // Iterates over each record from the collection polled from the topic and logs the key, value, partition and offset
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
