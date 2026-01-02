package cymo.project2.Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;


@Component
public class Producer {

    @Autowired
    private KafkaTemplate<String, String> producerTemplate;

    public void sendMessage(String value) {
        String key = "test1";
        producerTemplate.send("test1-input-topic", key, value);
    }


}