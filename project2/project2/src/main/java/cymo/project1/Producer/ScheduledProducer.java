package cymo.project2.Producer;


import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
public class ScheduledProducer {

    private final Producer producer;

    public ScheduledProducer(Producer producer) {
        this.producer = producer;
    }

    @Scheduled(fixedRate = 1000) // elke 1 seconde
    public void produceMessage() {
        producer.sendMessage("Hello world! " + System.currentTimeMillis());
    }
}
