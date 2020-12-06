package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Send message
        var producer = new KafkaProducer<String, String>(properties());
        var key = UUID.randomUUID().toString();
        // product_id , user_id, order_id
        var value = key+", 4312, 1456";
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);
        Callback callback = (data, ex ) -> {
            if(ex != null){
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucess sending " + data.topic() + ":::partition "+data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };
        // Send new mensage and print callback
        producer.send(record, callback).get();

        var email = "Thanks for your order! We are processing your order!";
        var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);
        producer.send(emailRecord, callback).get();
    }

    /**
     * Producer Config
     * @return
     */
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, StringSerializer.class.getName()+"-"+UUID.randomUUID().toString());
        // Record max/commit
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }
}
