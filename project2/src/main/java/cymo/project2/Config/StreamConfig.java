package cymo.project2.Config;


import Schema.Test;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.Map;

@Configuration
@EnableKafkaStreams
public class StreamConfig {

    @Value("${spring.kafka.producer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public KStream<String, Test> uppercaseStream(StreamsBuilder builder) {


        SpecificAvroSerde<Test> valueSerde = new SpecificAvroSerde<>();
        valueSerde.configure(Map.of("schema.registry.url", schemaRegistryUrl),false);


        KStream<String, Test> stream =
                builder.stream(
                        "test2-input-topic",Consumed.with(Serdes.String(),valueSerde));

        stream.mapValues(value -> {
                    if (value == null) return null;
                    return Test.newBuilder(value)
                            .setHelloworld(value.getHelloworld().toUpperCase())
                            .build();
                })
                .to("test2-output-topic",Produced.with(Serdes.String(),valueSerde));

        return stream;
    }

}
