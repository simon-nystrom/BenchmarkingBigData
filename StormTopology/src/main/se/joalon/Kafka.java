package se.joalon;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Kafka {

    private Properties props;

    public Kafka() {
        this.props = new Properties();
        props.put("bootstrap.servers", "192.168.56.101:6667");
    }

    public void setProperties(Properties props) {
        this.props = new Properties();
        this.props.put("bootstrap.servers", "192.168.56.101:6667");

        for (Map.Entry e : props.entrySet()) {
            this.props.put(e.getKey(), e.getValue());
        }
    }

    public void setConsumer() {
        Properties newProps = new Properties();

        newProps.put("group.id", "test");
        newProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        newProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        setProperties(newProps);
    }

    public void setProducer() {
        Properties newProps = new Properties();

        newProps.put("acks", "all");
        newProps.put("retries", 0);
        newProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        newProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        setProperties(newProps);

    }

    public void consumeMessages() {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(this.props);
        consumer.subscribe(Arrays.asList("test"));

        for (Map.Entry<String, List<PartitionInfo>> entry : consumer.listTopics().entrySet()) {
            System.out.println(entry);
        }

        while(true) {
            ConsumerRecords<String, String> recs = consumer.poll(100);
            for (ConsumerRecord<String, String> rec : recs) {
                System.out.println(rec.topic() + ", " + rec.key() + ": " + rec.value());
            }
        }
    }


    public void sendMessage (String topic, String message){

        KafkaProducer<String, String> producer = new KafkaProducer<>(this.props);

        System.out.println("Sending messages...");

        long start = System.nanoTime();

        for(int i = 0; i < 5000; i++) {
            producer.send(new ProducerRecord<String, String>(topic, i + message));
        }

        long end = System.nanoTime();
        long duration = (end - start) / 1000000;

        System.out.println("Done after " + duration + " millis!");
        producer.close();
    }
}
