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

// https://stackoverflow.com/questions/2921945/useful-example-of-a-shutdown-hook-in-java

/**
 * When the virtual machine begins its shutdown sequence it will start all registered shutdown hooks in some unspecified order and let them run concurrently.
 * When all the hooks have finished it will then run all uninvoked finalizers if finalization-on-exit has been enabled.
 * Finally, the virtual machine will halt.
 * <p>
 * <p>
 * KafkaConsumer.wakeup()
 * Exception used to indicate preemption of a blocking operation by an external thread.
 * For example, KafkaConsumer.wakeup() can be used to break out of an active KafkaConsumer.poll(long), which would raise an instance of this exception.
 * <p>
 * <p>
 * Basically, if you use consumer.poll(Integer.MAX_VALUE) the consumer will block until a message is fetched. In this case, if you would like to stop consumption you can call consumer.wakeup() (from an other thread) and catch the exception to shutdown properly.
 * <p>
 * Also, while commiting your offsets synchronously a call to consumer.wakeup() will throw a WakeupException .
 */

// That is, a shutdown hook keeps the JVM running until the hook has terminated (returned from the run() method.
public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka ConsumerDemoWithShutdown Class");

        String groupId = "java-app";
        String topic = "demo.java.topic";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Get a reference to the main thread
        // Current thread running the app
        final Thread mainThread = Thread.currentThread();

        // All this shutdown stuff allows for the program to shutdown correctly, finishing all the computing and connection to Kafka Topic

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

        // Knowing that consumer.poll will throw a WakeupException when shutting down
        // That's why it's needed to surround this with a catch to handle this expected exception
        try {
            consumer.subscribe(Arrays.asList(topic));

            // Poll for data
            while (true) {
                log.info("Polling data from Kafka Topic " + topic);

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
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
