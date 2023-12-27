package org.events;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;

public class EventProducer {
    private static final String TOPIC_NAME = "event-topic1";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            Random random = new Random();
            for (int i = 0; i < 10; i++) {
                String event = "Event-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, event);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            System.out.println("Sent record: " + event);
                        } else {
                            e.printStackTrace();
                        }
                    }
                });
                Thread.sleep(random.nextInt(2000)); // Introduce some delay between events
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
