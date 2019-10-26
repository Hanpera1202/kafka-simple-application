package com.example.simpleKafkaApplication;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Component
public class SimpleKafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaApplication.class);

    @Autowired
    private KafkaConsumer<String, String> consumer;

    @Autowired
    private SimpleKafkaProducer simpleKafkaProducer;


    public void runSingleWorker() {

        ObjectMapper mapper = new ObjectMapper();

        while(true) {

            ConsumerRecords<String, String> records = consumer.poll(1000);

            logger.info("-----start-----");

            Map<String, ArrayList<String>> datas = new HashMap<>();

            for (ConsumerRecord<String, String> record : records) {

                String message = record.value();

                logger.info("Received message: " + message);

                Map<String, Object> json;
                try {
                    json = mapper.readValue(message, new TypeReference<Map<String, Object>>() {});
                    String id = json.get("id").toString();
                    ArrayList<String> merged;
                    if (!datas.containsKey(id)) {
                        merged = new ArrayList<>();
                    } else {
                        merged = datas.get(id);
                    }
                    merged.add(json.get("message").toString());
                    datas.put(json.get("id").toString(), merged);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            logger.info("-----end-----");
            logger.info(datas.toString());

            for (Map.Entry<String, ArrayList<String>> entry : datas.entrySet()) {

                if (entry.getValue().size() >= 3) {
                    simpleKafkaProducer.send(entry);
                }

                // TODO What is this?
                //Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();
                //commitMessage.put(new TopicPartition(record.topic(), record.partition()),
                //        new OffsetAndMetadata(record.offset() + 1));

            }

            consumer.commitSync();
            logger.info("Offset committed to Kafka.");
        }
    }
}
