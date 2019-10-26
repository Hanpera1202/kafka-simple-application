package com.example.simpleKafkaApplication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Map;

@Component
public class SimpleKafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaProducer.class);

    @Autowired
    private KafkaProducer<String, String> producer;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    private ObjectMapper mapper = new ObjectMapper();

    /**
     * Function to send a message to Kafka
     * @param data The Map.Entry Object that we wish to send to the Kafka topic
     */
    public void send(Map.Entry<String, ArrayList<String>> data)
    {
        String payload = null;
        try {
            payload = mapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        logger.info("Sending Kafka message: " + data.toString());
        producer.send(new ProducerRecord<>(outputTopic, payload));
    }
}
