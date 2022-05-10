package narif.poc.springkstreampoc;

import lombok.extern.slf4j.Slf4j;
import narif.poc.springkstreampoc.exceptions.InvalidCreditCardException;
import narif.poc.springkstreampoc.model.OrderInputMsg;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.KafkaStreamsCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.messaging.Message;

import java.util.Objects;
import java.util.function.Consumer;
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
                .transform(() -> new Transformer<String, OrderInputMsg, KeyValue<String, OrderInputMsg>>() {
                    ProcessorContext context;
                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                    }

                    @Override
                    public KeyValue<String, OrderInputMsg> transform(String key, OrderInputMsg value) {
                        try{
                            final OrderInputMsg msg = OrderProcessorService.processOrderMsg(value);
                            return new KeyValue<>(key, msg);
                        }catch (Exception e){
                            System.err.println("===================ERROR: "+e.getMessage());
                            context.commit();
                            return new KeyValue<>(key, null);
                        }
                    }

                    @Override
                    public void close() {}
                })
                .peek((key, value) -> log.info("ORDER CREDIT CARD INFO MASKED FOR KEY: {}, VALUE:{}", key, value))
                .filter((key, value) -> value!=null);

//                .mapValues(OrderProcessorService::processOrderMsg);
    }

    @Bean
    public Function<KStream<String, String>, KStream<String, String>> upperCaseProcessor(){
        return stringStringKStream -> stringStringKStream
                .mapValues(this::getToUpperCase);
    }

    private String getToUpperCase(String value) {
        return value.toUpperCase();
    }

}
