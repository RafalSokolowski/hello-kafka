package pl.rav.trivial.launcher;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import pl.rav.trivial.topology.MyTopology;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static pl.rav.trivial.config.ConstConfig.DESTINATION_TOPIC;
import static pl.rav.trivial.config.ConstConfig.PARTITIONS_NUMBER;
import static pl.rav.trivial.config.ConstConfig.REPLICAS_NUMBER;
import static pl.rav.trivial.config.ConstConfig.SOURCE_TOPIC;

@Slf4j
public class StreamApp {

    public static void main(String[] args) {

        // 1. Bootstrap server configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // read the records that's available in the Kafka topic. After this application started, it's going to be latest
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);      // default serialization for key, then serialization on the Consumed/Produced.with() can be turned off
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);    // default serialization for value

        // 2. Create Kafka topics
        createTopics(props, List.of(SOURCE_TOPIC, DESTINATION_TOPIC));

        // 3. Access to the Topology
        Topology topology = MyTopology.buildTopology();

        try {
            KafkaStreams kafkaStream = new KafkaStreams(topology, props);

            // make sure release all the resources, get showdown hoot - any time the app is close the stream close is executed and all the resources release
            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStream::close));

            // Start the application
            kafkaStream.start();

        } catch (Exception e) {
            log.error("Start failed: {}", e.getMessage());
        }

    }

    private static void createTopics(Properties properties, List<String> kafkaTopicsNames) {

        AdminClient adminClient = AdminClient.create(properties);

        CompletableFuture<Set<String>> existingTopicsFuture = adminClient
                .listTopics()
                .names()
                .toCompletionStage()
                .toCompletableFuture();

        // quick and dirty refactor to not create topics with the same name (do not need manually remove topics in container - WIP for development only)
        existingTopicsFuture
                .thenApply(existingTopicsNames -> kafkaTopicsNames
                        .stream()
                        .filter(newTopicName -> !existingTopicsNames.contains(newTopicName))
                        .map(name -> new NewTopic(name, PARTITIONS_NUMBER, REPLICAS_NUMBER))
                        .toList())
                .thenAccept(adminClient::createTopics)
                .join();
    }

}
