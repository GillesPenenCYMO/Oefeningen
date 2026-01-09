package comy.Producer;


import Schema.HelloWorld;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class HelloWorldAvroProducer {

    private final KafkaTemplate<String, HelloWorld> kafkaTemplate;

    public HelloWorldAvroProducer(KafkaTemplate<String, HelloWorld> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRate = 1000)
    public void sendMessage() {
        String tijd = String.valueOf(System.currentTimeMillis());
        HelloWorld helloWorld = HelloWorld.newBuilder()
                .setHelloworld("helloworld")
                .setTijd(tijd)
                .build();
        kafkaTemplate.send("test3-HelloInput-topic", "test3", helloWorld);

    }
}
