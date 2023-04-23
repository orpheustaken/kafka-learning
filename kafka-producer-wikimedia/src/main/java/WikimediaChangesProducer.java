import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException, StreamException {

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "wikimedia.recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        // Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer Object
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Event Handler and Source
        // Uses the okhttp lib to retrieve data from the URI and produce to the topic

        // The class WikimediaChangeHandler manages the producement by implementing a BackgroundEventHandler Interface
        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

        // Event Source and Handler properties
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(URI.create(url));
        BackgroundEventSource.Builder backgroundEventSourceBuilder = new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder);

        // Builds the EventSource based on properties
        BackgroundEventSource backgroundEventSource = backgroundEventSourceBuilder.build();

        // Start Producer by calling the backgroundEventSource, where the producer message sending is defined
        backgroundEventSource.start();

        // Produce for 30 segunds then stops
        // Without this the Main thread would exit immediately after starting another thread, without producing any messages
        TimeUnit.SECONDS.sleep(30);
    }
}
