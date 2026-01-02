package cymo.project2;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class Stream1Test {


    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @Test
    void testStream() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-uppercase");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("test1-input-topic", Consumed.with(Serdes.String(), Serdes.String()));
        stream.mapValues(value -> value == null ? null : value.toUpperCase())
                .to("test1-output-topic");

        Topology topology = builder.build();

        testDriver = new TopologyTestDriver(topology, props);


        inputTopic = testDriver.createInputTopic("test1-input-topic", Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = testDriver.createOutputTopic("test1-output-topic", Serdes.String().deserializer(), Serdes.String().deserializer());
        inputTopic.pipeInput("key1", "hello");
        inputTopic.pipeInput("key2", "world");

        assertEquals("HELLO", outputTopic.readValue());
        assertEquals("WORLD", outputTopic.readValue());

    }
}