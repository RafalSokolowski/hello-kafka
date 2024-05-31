package pl.rav.trivial.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import static pl.rav.trivial.config.ConstConfig.DESTINATION_TOPIC;
import static pl.rav.trivial.config.ConstConfig.PROCESS;
import static pl.rav.trivial.config.ConstConfig.SOURCE_TOPIC;

public class MyTopology {

    /**
     * The Topology class represents the computational logic of a Kafka Streams application,
     * defining how data should be processed from the source topic to the destination topic
     * through the various transformations.
     * */
    public static Topology buildTopology() {

        // Use stream builder to build the pipeline,
        // here, reading from the Kafka topic, perform processing and publish/write the data to another Kafka topic
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // 1. Read value form the topic <- Consumer API under the hood
        KStream<String, String> kafkaStream = streamsBuilder.stream(SOURCE_TOPIC
//                , Consumed.with(
//                    Serdes.String(),    // Key serializer (value as String)
//                    Serdes.String())     // Key deserializer
        );

        // put what was read to the terminal log
        kafkaStream.print(Printed.<String, String>toSysOut().withLabel("ReadFromTheTopic_" + SOURCE_TOPIC));

        // 2. Build business processing logic - apply needed transformation / processing
        KStream<String, String>  processedStream = kafkaStream

//                .filter((key, value) -> value.length() > 5)
//                .filterNot((key, value) -> value.length() > 5)  // ignore any values higher than length 5
//                .mapValues((readOnlyKey, value) -> value + PROCESS)

                .map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()))  // allow to modify key and value, not only value

//                .flatMap()
//                .flatMapValues()
        ;

        // kStream1.merge(kStream2) - can sent messages from a couple of sources to one destination

        // put what was processed/modified to the terminal log
        processedStream.print(Printed.<String, String>toSysOut().withLabel("ProcessedForTopic_" + DESTINATION_TOPIC));

        // 3. publish the value to another topic <- Producer API under the hood
        processedStream.to(DESTINATION_TOPIC
//                , Produced.with(
//                    Serdes.String(),    // Key serializer (value as String)
//                    Serdes.String())     // Key deserializer
        );

        return streamsBuilder.build();
    }

}
