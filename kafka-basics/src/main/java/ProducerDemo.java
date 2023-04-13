import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka ProducerDemo Class");

        // Create Producer Properties
        // Connect to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // Set Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create Producer Record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo.java.topic", "Sample data sent to Kafka Topic by Java code via Kafka Streams API");

        // Send Data -- Asynchronously
        producer.send(producerRecord);

        // Flush and Close Producer
        // Flush tells the Producer to send all data and block until done. -- Synchronously
        producer.flush();
        // Flushes and Closes the Producer
        producer.close();
    }
}
