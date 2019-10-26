package com.example.simpleKafkaApplication.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Properties;

@Configuration
public class KafkaClients {

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Value("${kafka.consumer.groupId}")
    private String consumerGroupId;

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Bean
    public KafkaConsumer<String, String> kafkaConsumer() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaBootstrapServers);
        consumerProps.put("group.id", consumerGroupId);
        consumerProps.put("zookeeper.session.timeout.ms", "6000");
        consumerProps.put("zookeeper.sync.time.ms", "2000");
        consumerProps.put("auto.commit.enable", "false");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("consumer.timeout.ms", "-1");
        consumerProps.put("max.poll.records", "10000");
        consumerProps.put("fetch.min.bytes", "1000000");
        consumerProps.put("fetch.max.wait.ms", "1000");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);
        kafkaConsumer.subscribe(Arrays.asList(inputTopic));
        return kafkaConsumer;
    }

    @Bean
    public KafkaProducer<String, String> kafkaProducer() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafkaBootstrapServers);
        producerProps.put("acks", "all");
        producerProps.put("retries", 0);
        producerProps.put("batch.size", 16384);
        producerProps.put("linger.ms", 1);
        producerProps.put("buffer.memory", 33554432);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(producerProps);
    }

}
