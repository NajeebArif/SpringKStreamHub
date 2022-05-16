package narif.poc.springkstreampoc;

import lombok.extern.slf4j.Slf4j;
import narif.poc.springkstreampoc.model.OrderInputMsg;
import narif.poc.springkstreampoc.model.Tuple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Map;
import java.util.function.Function;

@SpringBootApplication
@Slf4j
public class SpringKStreamPocApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKStreamPocApplication.class, args);
    }

    @Bean
    public Function<KStream<String, OrderInputMsg>, KStream<String, OrderInputMsg>[]> orderBranchingProcessor(){
        Predicate<String, Tuple<Throwable, OrderInputMsg>> isSuccessful = (key, value) -> value.getOptionalT().isEmpty();
        Predicate<String, Tuple<Throwable, OrderInputMsg>> isFailure = (key, value) -> value.getOptionalT().isPresent();
        return stringOrderInputMsgKStream -> {
            final Map<String, KStream<String, Tuple<Throwable, OrderInputMsg>>> stringKStreamMap = stringOrderInputMsgKStream
                    .peek((key, value) -> log.info("Branch Order input msg received with key: {} and payload: {}", key, value))
                    .map(this::getTransformedMessage)
                    .split()
                    .branch(isSuccessful)
                    .branch(isFailure)
                    .noDefaultBranch();
            return stringKStreamMap.values()
                    .stream()
                    .map(this::getStringOrderInputMsgKStream)
                    .toArray(KStream[]::new);
        };
    }

    private KStream<String, OrderInputMsg> getStringOrderInputMsgKStream(KStream<String, Tuple<Throwable, OrderInputMsg>> stringTupleKStream) {
        return stringTupleKStream
                .mapValues((readOnlyKey, value) -> value.getOptionalU()
                        .orElseGet(OrderInputMsg::new));
    }

    private KeyValue<String, Tuple<Throwable, OrderInputMsg>> getTransformedMessage(String key, OrderInputMsg value) {
        try{
            final OrderInputMsg msg = OrderProcessorService.processOrderMsg(value);
            final Tuple<Throwable, OrderInputMsg> inputMsgTuple = new Tuple<>(null, msg);
            return new KeyValue<>(key, inputMsgTuple);
        }catch (Exception e){
            final Tuple<Throwable, OrderInputMsg> inputMsgTuple = new Tuple<>(e, value);
            return new KeyValue<>(key, inputMsgTuple);
        }
    }

    @Bean
    public Function<KStream<String, String>, KStream<String, String>[]> upperCaseProcessor(){
        return stringStringKStream -> {
            final Map<String, KStream<String, String>> first = stringStringKStream
                    .peek((key, value) -> log.info("TEXT MSG READ."))
                    .mapValues(value -> value.toUpperCase())
                    .split()
                    .branch((key, value) -> value.contains("FIRST"))
                    .branch((key, value) -> value.contains("SECOND"))
                    .noDefaultBranch();
            return first.values()
                    .stream().map(stringStringKStream1 -> stringStringKStream1.mapValues((readOnlyKey, value) -> value.toLowerCase()))
                    .toArray(KStream[]::new);
        };
    }

}
