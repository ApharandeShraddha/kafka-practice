package com.kafka.consumer;

import com.fasterxml.jackson.core.json.UTF8StreamJsonParser;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    //elastic search credentials
    //create elastic search instance and add credentials
    private static final String HOSTNAME = "";
    private static final String USERNAME = "";
    private static final String PASSWORD = "";

    //kafka consumer credentials
    private static final String bootStrapServers = "127.0.0.1:9092";
    private static final String groupId = "kafka-twitter-elasticsearch-1";
    private static final String offset_config = "earliest";

    private static final String TOPIC_NAME = "twitter_tweets";


    private static RestHighLevelClient createClient() {

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(USERNAME, PASSWORD));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(HOSTNAME, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    private static KafkaConsumer<String, String> createConsumer(String topicName) {

        //create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset_config);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        //create consumer
        KafkaConsumer<String, String> twitterConsumer = new KafkaConsumer<String, String>(properties);

        //subscribe to consumer
        twitterConsumer.subscribe(Arrays.asList(TOPIC_NAME));

        return twitterConsumer;
    }


    public static void main(String[] args) throws IOException {

        RestHighLevelClient elasticSearchClient = createClient();

        KafkaConsumer<String, String> twitterConsumer = createConsumer(TOPIC_NAME);

        //poll for new data
        while (true) {
            ConsumerRecords<String, String> records = twitterConsumer.poll(Duration.ofMillis(100));

            BulkRequest bulkRequest = new BulkRequest();

            int recordCount = records.count();

            logger.info("Received "+ recordCount +" records.");

            for (ConsumerRecord<String, String> record : records) {

                try {
                    String id = extractIdFromTweet(record.value());

                    IndexRequest indexRequest = new IndexRequest("twitter")
                            .source(record.value(), XContentType.JSON)
                            .id(id);
                    bulkRequest.add(indexRequest);
                }catch (NullPointerException e ){
                    logger.warn("Skipping bad data - "+record.value());
                }
            }
            if (recordCount > 0) {
                BulkResponse bulkItemResponses = elasticSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("committing offsets.");
                twitterConsumer.commitAsync();
                logger.info("Offsets have been committed.");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private static String extractIdFromTweet(String tweetsJson) {
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(tweetsJson).getAsJsonObject().get("id_str").toString();
    }
}
