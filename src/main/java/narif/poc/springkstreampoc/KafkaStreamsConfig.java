package narif.poc.springkstreampoc;

import lombok.extern.slf4j.Slf4j;
import narif.poc.springkstreampoc.exceptions.InvalidCreditCardException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@Slf4j
public class KafkaStreamsConfig {

    //    @Bean
//    public StreamsBuilderFactoryBeanConfigurer streamsCustomizer() {
//        return new StreamsBuilderFactoryBeanConfigurer() {
//
//            @Override
//            public void configure(StreamsBuilderFactoryBean factoryBean) {;
//                factoryBean.setStreamsUncaughtExceptionHandler(getStreamsUncaughtExceptionHandler());
//            }
//
//            @Override
//            public int getOrder() {
//                return Integer.MAX_VALUE - 10000;
//            }
//
//        };
//    }

    @Bean
    @Primary
    public StreamsBuilderFactoryBeanCustomizer streamsBuilderFactoryBeanCustomizer() {
        return factoryBean -> {
            factoryBean.setStreamsUncaughtExceptionHandler(getStreamsUncaughtExceptionHandler());
//            factoryBean.setKafkaStreamsCustomizer(kafkaStreams ->
//                    kafkaStreams.setUncaughtExceptionHandler(getStreamsUncaughtExceptionHandler()));
        };
    }

//    @Bean
//    public StreamsBuilderFactoryBeanCustomizer streamsBuilderFactoryBeanCustomizer() {
//        return factoryBean -> {
//            if (isOrderProcessorStream(factoryBean)) {
//                factoryBean.setKafkaStreamsCustomizer(kafkaStreams ->
//                        kafkaStreams.setUncaughtExceptionHandler(getStreamsUncaughtExceptionHandler()));
//            }
//        };
//    }

    private StreamsUncaughtExceptionHandler getStreamsUncaughtExceptionHandler() {
        return exception -> {
            Throwable cause = exception.getCause();
            if (cause.getClass().equals(InvalidCreditCardException.class)) {
                log.error(cause.getMessage());
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
            }
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        };
    }

//    private boolean isOrderProcessorStream(StreamsBuilderFactoryBean factoryBean) {
//        return Objects.requireNonNull(factoryBean.getStreamsConfiguration()).getProperty(StreamsConfig.APPLICATION_ID_CONFIG)
//                .equals("OrderProcessorStream");
//    }

}
