import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka ProducerDemoKeys Class");

        // Create Producer Properties
        // Connect to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // Set Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Round Robin sends messages randomly to partitions
        // But when sending multiple messages at the same time, Sticky Partitioner is used instead
        // Sticky Partitioner, all messages are sent in one batch to the same partition, this improves performance
        for (int i = 0; i < 10; i++) {
            String topicName = "demo.java.topic";
            // A same key would go to the same partition
            String key = "uuid_" + UUID.randomUUID();
            String value = "\"Data sent with key. Index: " + i + "\"";

            // Create Producer Record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topicName, key, value);

            // Send Data -- Asynchronously
            // Callback allows the Producer to have the metadata to where the message was sent, if successful
            // Also shows an expection when errors occur
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // Executes every time a record is successfully sent or an exception is thrown
                    if (exception == null) {
                        // The record was successfully sent
                        log.info("Message Metadata \n" +
                                "Key: " + key + "\n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing", exception);
                    }
                }
            });
        }

        // Flush and Close Producer
        // Flush tells the Producer to send all data and block until done. -- Synchronously
        producer.flush();
        // Flushes and Closes the Producer
        producer.close();
    }
}
