package narif.poc.springkstreampoc;

import lombok.extern.slf4j.Slf4j;
import narif.poc.springkstreampoc.model.OrderInputMsg;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Function;

@SpringBootApplication
@Slf4j
public class SpringKStreamPocApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKStreamPocApplication.class, args);
    }

    @Bean
    public Function<KStream<String, OrderInputMsg>, KStream<String, OrderInputMsg>> orderProcessor(){
        return stringOrderInputMsgKStream -> stringOrderInputMsgKStream
                .peek((key, value) -> log.info("Order input msg received with key: {} and payload: {}", key, value))
                .mapValues(OrderProcessorService::processOrderMsg);
    }

    @Bean
    public Function<KStream<String, String>, KStream<String, String>> upperCaseProcessor(){
        return stringStringKStream -> stringStringKStream
                .mapValues((ValueMapper<String, String>) String::toUpperCase);
    }

}
