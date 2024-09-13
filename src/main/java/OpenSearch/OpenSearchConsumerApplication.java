package OpenSearch;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@EnableKafka
public class OpenSearchConsumerApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumerApplication.class);

    private final RestHighLevelClient openSearchClient;

    @Autowired
    public OpenSearchConsumerApplication(RestHighLevelClient openSearchClient) {
        this.openSearchClient = openSearchClient;
    }

    public static void main(String[] args) {
        SpringApplication.run(OpenSearchConsumerApplication.class, args);
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-opensearch-demo");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Override
    public void run(String... args) throws Exception {
        createIndexIfNotExists();
    }

    private void createIndexIfNotExists() {
        try {
            if (!openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT)) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia Index has been created!");
            } else {
                log.info("The Wikimedia Index already exists");
            }
        } catch (IOException e) {
            log.error("Error creating OpenSearch index", e);
            // Handle the exception appropriately (log, retry, etc.)
        }
    }

    @KafkaListener(topics = "wikimedia.recentchange", groupId = "consumer-opensearch-demo")
    public void listen(ConsumerRecord<String, String> record) throws InterruptedException, IOException {
        BulkRequest bulkRequest = new BulkRequest();

        try {
            String id = extractId(record.value());

            IndexRequest indexRequest = new IndexRequest("wikimedia")
                    .source(record.value(), XContentType.JSON)
                    .id(id);

            bulkRequest.add(indexRequest);

        } catch (Exception e) {
            log.error("Error processing record: {}", e.getMessage());
        }

        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            log.info("Inserted {} record(s).", bulkResponse.getItems().length);

            Thread.sleep(1000);
        }
    }

    private static String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}