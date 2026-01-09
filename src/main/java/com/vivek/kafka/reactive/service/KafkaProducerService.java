package com.vivek.kafka.reactive.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic}")
    private String topic;

    public <T> Mono<Void> sendMessage(String key, T message) {
        return Mono.fromRunnable(() -> {
                    try {
                        String json = objectMapper.writeValueAsString(message);
                        kafkaTemplate.send(topic, key, json);
                        log.info("Kafka event sent: {}", json);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }
}
