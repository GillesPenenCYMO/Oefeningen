package comy.Producer;





import Schema.Number;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


@Service
public class AvroProducer {

    private final KafkaTemplate<String, Number> kafkaTemplate;

    public AvroProducer(KafkaTemplate<String, Number> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRate = 1000)
    public void sendMessage() {
        Number number = Number.newBuilder()
                .setHelloworld("helloworld")
                .setNumber(RandomUtils.nextInt())
                .build();
        kafkaTemplate.send("test3-NumberInput-topic", "test3", number);
    }
}