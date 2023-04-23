import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        // Build a URI from connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // Extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // Rest client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));
        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "wikimedia-java-app";
        String bootstrapServer = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServer);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        return new KafkaConsumer<>(properties);
    }


    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        // Create OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // Create Kafka Consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

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

        // Create index on OpenSearch if it doesn't exist
        try (openSearchClient) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Wikimedia Index has been created");
            } else {
                log.info("Wikimedia Index already exists");
            }
        }

        // Subscribe Consumer to topic
        consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

        // Knowing that consumer.poll will throw a WakeupException when shutting down
        // That's why it's needed to surround this with a catch to handle this expected exception
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received " + recordCount + " records");

                for (ConsumerRecord<String, String> record : records) {
                    log.info(record.value());
                }

                for (ConsumerRecord<String, String> record : records) {
                    // Send record into OpenSearch
                    IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON);

                    // Not working for some unholy reason
//                IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
//                log.info(response.getId());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        }
        // This catch, otherwise, is to handle a unexpected exception...
        catch (Exception e) {
            log.error("Unexpected exception...", e);
        } finally {
            // This is to actually close the connection properly, after all exceptions
            consumer.close(); // This will also commit offsets
            openSearchClient.close();
            log.info("The consumer is now gracefully handling shutting down");
        }
    }
}
