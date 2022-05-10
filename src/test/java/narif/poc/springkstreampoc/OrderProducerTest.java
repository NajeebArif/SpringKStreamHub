package narif.poc.springkstreampoc;

import lombok.SneakyThrows;
import narif.poc.springkstreampoc.model.OrderInputMsg;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;
import org.springframework.test.context.ActiveProfiles;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@SpringBootTest(classes = {SpringKStreamPocApplication.class})
@ActiveProfiles("test")
public class OrderProducerTest {

    public static final String RAW_ORDER_TOPIC = "raw-order-topic";
    public static final String bootstrapServers = "localhost:29092";
    private KafkaTemplate<String, OrderInputMsg> orderInputMsgKafkaTemplate;

    @BeforeEach
    public void init(){
        orderInputMsgKafkaTemplate = kafkaTemplate();
    }

    @Test
    void produceData(){
        OrderInputMsg orderInputMsg = getOrderInputMsg();
        String key = UUID.randomUUID().toString();
        orderInputMsgKafkaTemplate.send(RAW_ORDER_TOPIC, key, orderInputMsg);
    }

    @Test
    void produceMessage(){
        OrderInputMsg orderInputMsg = getOrderInputMsg();
        String key = UUID.randomUUID().toString();
        Message<OrderInputMsg> orderInputMsgMessage = MessageBuilder.withPayload(orderInputMsg)
                .setHeader(KafkaHeaders.MESSAGE_KEY, key)
                .setHeader(KafkaHeaders.TOPIC, RAW_ORDER_TOPIC)
                .setCorrelationId(UUID.randomUUID()).build();
        orderInputMsgKafkaTemplate.send(orderInputMsgMessage);
    }

    @Test
    void produce50Messages(){
        for (int i = 0; i < 49; i++) {
            OrderInputMsg orderInputMsg = getOrderInputMsg();
            if(i==2)
                orderInputMsg.setCreditCardNumber("magic");
            String key = UUID.randomUUID().toString();
            orderInputMsgKafkaTemplate.send(RAW_ORDER_TOPIC, key, orderInputMsg);
            sleeeep();
        }
    }

    @SneakyThrows
    private void sleeeep() {
        Thread.sleep(2000);
    }

    private OrderInputMsg getOrderInputMsg() {
        OrderInputMsg orderInputMsg = new OrderInputMsg();
        orderInputMsg.setOrderId(UUID.randomUUID().toString());
        orderInputMsg.setOrderAmount(1000d);
        orderInputMsg.setCreditCardNumber("1111-2222-3333-4444");
        orderInputMsg.setItemName("PS5");
        orderInputMsg.setUserName("NJ");
        return orderInputMsg;
    }

    private Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return props;
    }

    private ProducerFactory<String, OrderInputMsg> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    private KafkaTemplate<String, OrderInputMsg> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
