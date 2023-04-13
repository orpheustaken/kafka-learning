import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka ProducerDemoWithCallback Class");

        // Create Producer Properties
        // Connect to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // Set Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//        properties.setProperty("batch.size", "400");
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 5; i++) {
            // Round Robin sends messages randomly to partitions
            // But when sending multiple messages at the same time, Sticky Partitioner is used instead
            // Sticky Partitioner, all messages are sent in one batch to the same partition, this improves performance
            for (int j = 0; j < 10; j++) {
                // Create Producer Record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo.java.topic", "Sample data sent to Kafka Topic by Java code via Kafka Streams API in a for loop. Index: " + j + " Loop: " + i);

                // Send Data -- Asynchronously
                // Callback allows the Producer to have the metadata to where the message was sent, if successful
                // Also shows an expection when errors occur
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // Executes every time a record is successfully sent or an exception is thrown
                        if (exception == null) {
                            // The record was successfully sent
                            log.info("Received new metadata \n" +
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

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // Flush and Close Producer
        // Flush tells the Producer to send all data and block until done. -- Synchronously
        producer.flush();
        // Flushes and Closes the Producer
        producer.close();
    }
}
