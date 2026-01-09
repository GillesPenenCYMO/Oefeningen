package cymo.project2;

import cymo.project2.Config.StreamConfig;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class Stream2Test {


    private TopologyTestDriver testDriver;
    private StreamsBuilderFactoryBean  factoryBean;
    private SpecificAvroSerde<Schema.Test> valueSerde;

    private TestInputTopic<String, Schema.Test> inputTopic;
    private TestOutputTopic<String, Schema.Test> outputTopic;


    @BeforeEach
    public void setup() {

        MockSchemaRegistryClient mockClient = new MockSchemaRegistryClient();
        valueSerde = new SpecificAvroSerde<>(mockClient);
        valueSerde.configure(Map.of("schema.registry.url", "mock://test"), false);

        Properties props = factoryBean.getStreamsConfiguration();

        Topology topology = factoryBean.getTopology();
        testDriver = new TopologyTestDriver(topology, props);


        inputTopic = testDriver.createInputTopic(
                "test2-input-topic",
                Serdes.String().serializer(),
                valueSerde.serializer()
        );
        outputTopic = testDriver.createOutputTopic(
                "test2-output-topic",
                Serdes.String().deserializer(),
                valueSerde.deserializer()
        );

    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void testStream() {
        Schema.Test input1 = Schema.Test.newBuilder().setHelloworld("hello").build();
        Schema.Test input2 = Schema.Test.newBuilder().setHelloworld("world").build();

        inputTopic.pipeInput("key1", input1);
        inputTopic.pipeInput("key2", input2);

        assertEquals("HELLO", outputTopic.readValue().getHelloworld());
        assertEquals("WORLD", outputTopic.readValue().getHelloworld());

    }
}