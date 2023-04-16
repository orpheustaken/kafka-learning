import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka ConsumerDemoWithShutdown Class");

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

        // Get a reference to the main thread
        // Current thread running the app
        final Thread mainThread = Thread.currentThread();

        // All this shutdown stuff allows for the program to shutdown correctly, finishing all the computing and connection to Kafka Topic
        // Kinda like signal in C?

        // Adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                // A new thread was created to handle the shutting down
                log.info("Detected a shutdown, will exit by calling consumer.wakeup()...");
                // Next time consumer.poll() is run, it will throw a wakeup exception
                consumer.wakeup();

                // Join the main thread to allow the execution of the code in the main thread
                try {
                    // This joins both the new thread and the main one
                    // To allow the program to finish everything before actually shutting down
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
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
        // Knowing that consumer.poll will throw a WakeupException when shutting down
        // That's why is needed to surround this with a catch to handle this expected exception
        catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        }
        // This catch, otherwise, is to handle a unexpected exception...
        catch (Exception e) {
            log.error("Unexpected exception...");
        } finally {
            // This is to actually close the connection properly, after all exceptions
            consumer.close(); // This will also commit offsets
            log.info("The consumer is now gracefully handling shutting down");
        }
    }
}
