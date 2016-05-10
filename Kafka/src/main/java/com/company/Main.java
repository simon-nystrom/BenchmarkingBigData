package com.company;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.SystemTime;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

public class Main {

    public static final int NUMBER_OF_MESSAGES = 20000;
    public static final int NUMBER_OF_TESTS = 20;

    public static final String KAFKA_BOOTSTRAP_SERVER = "192.168.99.100:6667";
    public static final String KAFKA_XBRL_TOPIC = "xbrl_messages";

    public static void main(String[] args) throws IOException {

        // Kafka producer
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVER);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 1);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        String message = new String(Files.readAllBytes(Paths.get("BIPAB.xml")));


        long start = System.nanoTime();
        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            producer.send(new ProducerRecord<String, String>("test1" , Integer.toString(i), message));
        }

        producer.close();

        // Kafka consumer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVER);
        properties.put("group.id", "test1");
        properties.put("client.id", "2");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("time"));

        long end = 0;

        while (end == 0) {
            ConsumerRecords<String, String> records = consumer.poll(1);
            for (ConsumerRecord<String, String> record : records) {
                end = System.nanoTime();
                System.out.println(record.toString());
            }
        }

        System.out.println((end - start) / 1000000 + " ms");
    }
}
