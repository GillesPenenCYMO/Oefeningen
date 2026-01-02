package cymo.project2.Config;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class StreamConfig {

    @Bean
    public KafkaStreams kafkaStreams() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "uppercase-stream-app"); //
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Stream bouwen
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream("test1-input-topic");

        inputStream
                .mapValues(value -> value == null ? null : value.toUpperCase())
                .to("test1-output-topic");

        // KafkaStreams instantie aanmaken
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;
    }
}
//@Configuration
//public class StreamConfig {
//
//    @Bean
//    public StreamsConfig streamsConfig() {
//        Properties props = new Properties();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "project1");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        return new StreamsConfig(props);
//    }

//    @Bean
//    public KStream<String, String> kStream(StreamsConfig streamsConfig) {
//        KStream<String, String> stream = builder.stream("test1-input-topic");
//        stream.mapValues(value -> value == null ? null : value.toUpperCase())
//                .to("test1-output-topic");
//        return stream;
//    }
//
//}