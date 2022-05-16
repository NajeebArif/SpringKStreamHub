package narif.poc.springkstreampoc;

import narif.poc.springkstreampoc.model.OrderInputMsg;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CustomKafkaProducer {

    @Test
    void produceData(){
        Properties props = getKafkaProducerProps();
        Producer<String, String> producer = new KafkaProducer<>(props);
        String topicName="src-textMsg-topic";
        for(int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<>(topicName,Integer.toString(i), Integer.toString(i)));
        System.out.println("Message Sent.");
        producer.close();
    }

    @Test
    void produceOrderInfo() throws ExecutionException, InterruptedException {
        Properties kafkaProducerProps = getKafkaProducerProps();
        kafkaProducerProps.put("value.serializer",
                "org.springframework.kafka.support.serializer.JsonSerializer");
        Producer<String, OrderInputMsg> producer = new KafkaProducer<>(kafkaProducerProps);
        String topicName = OrderProducerTest.RAW_ORDER_TOPIC;
        for (int i = 0; i < 10; i++) {
            OrderInputMsg orderInputMsg = getOrderInputMsg(i);
            if(i==2)
                orderInputMsg.setCreditCardNumber("magic");
            producer.send(new ProducerRecord<>(topicName, UUID.randomUUID().toString(),orderInputMsg )).get();
            System.out.println("MSG SENT.");
        }
        producer.close();
    }

    private OrderInputMsg getOrderInputMsg(int i) {
        OrderInputMsg orderInputMsg = new OrderInputMsg();
        orderInputMsg.setOrderId(UUID.randomUUID().toString());
        orderInputMsg.setOrderAmount(1000d);
        orderInputMsg.setCreditCardNumber("1111-2222-3333-5555");
        orderInputMsg.setItemName("PS5 "+i);
        orderInputMsg.setUserName("Najeeb");
        return orderInputMsg;
    }

    private Properties getKafkaProducerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("acks", "all");
        props.put("retries", 0);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
