package cymo.project1;

import cymo.project1.Config.StreamConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest
@EnableKafkaStreams
public class Stream1Test {

    @Autowired
    private StreamsBuilderFactoryBean factoryBean;

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @BeforeEach
    void setup() throws Exception {
        Properties props = factoryBean.getStreamsConfiguration();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        Topology topology = factoryBean.getObject().build();

        testDriver = new TopologyTestDriver(topology, props);
    }
    @AfterEach
    void tearDown(){
        testDriver.close();
    }

        @Test
        void testStream() {
            inputTopic = testDriver.createInputTopic("test1-input-topic", Serdes.String().serializer(), Serdes.String().serializer());
            outputTopic = testDriver.createOutputTopic("test1-output-topic", Serdes.String().deserializer(), Serdes.String().deserializer());
            inputTopic.pipeInput("key1", "hello");
            inputTopic.pipeInput("key2", "world");

            assertEquals("HELLO", outputTopic.readValue());
            assertEquals("WORLD", outputTopic.readValue());

        }
    }