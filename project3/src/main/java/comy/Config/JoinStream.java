package comy.Config;

import Schema.HelloWorld;
import Schema.JoinedHelloNumber;
import Schema.Number;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Duration;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class JoinStream {

    @Value("${spring.kafka.producer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public KStream<String, JoinedHelloNumber> streamjoin(StreamsBuilder builder) {


        Map<String, String> serdeConfig =
                Map.of("schema.registry.url", schemaRegistryUrl);

        SpecificAvroSerde<HelloWorld> helloSerde = new SpecificAvroSerde<>();
        helloSerde.configure(serdeConfig, false);

        SpecificAvroSerde<Schema.Number> numberSerde = new SpecificAvroSerde<>();
        numberSerde.configure(serdeConfig, false);

        SpecificAvroSerde<JoinedHelloNumber> joinedSerde = new SpecificAvroSerde<>();
        joinedSerde.configure(serdeConfig, false);


        KStream<String, HelloWorld> helloStream =
                builder.stream("test3-HelloInput-topic",
                        Consumed.with(Serdes.String(), helloSerde));

        KStream<String, Number> numberStream =
                builder.stream("test3-NumberInput-topic",
                        Consumed.with(Serdes.String(), numberSerde));

        KStream<String, JoinedHelloNumber> joinedStream =
                helloStream.join(
                        numberStream,
                        (hello, number) ->
                                JoinedHelloNumber.newBuilder()
                                        .setHelloworld(hello.getHelloworld())
                                        .setTijd(hello.getTijd())
                                        .setNumber(number.getNumber())
                                        .build(),
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(2000)),
                        StreamJoined.with(Serdes.String(),
                                helloSerde, numberSerde)
                );

        joinedStream.to(
                "test3-output-topic",
                Produced.with(Serdes.String(), joinedSerde)
        );

        return joinedStream;
    }

}