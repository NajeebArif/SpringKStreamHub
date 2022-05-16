package narif.poc.springkstreampoc;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
//@Disabled
class SpringKStreamPocApplicationTests {

    @Autowired
    private KafkaTemplate<String, String> stringStringKafkaTemplate;

    @Value("${test-input-topic}")
    private String inputTopic;

    @Test
    void contextLoads() {
        for (int i = 0; i < 10; i++) {
            String data = "My First Message.";
            if(i%2==0) {
                data = "My Second Message";
            }
            stringStringKafkaTemplate.send(inputTopic, data);
            System.out.println("MESSAGE SENT.");
        }
    }

}
