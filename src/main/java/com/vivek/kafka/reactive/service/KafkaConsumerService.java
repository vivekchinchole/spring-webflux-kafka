package com.vivek.kafka.reactive.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.vivek.kafka.reactive.entity.Product;
import com.vivek.kafka.reactive.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final ProductRepository productRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "product-events", groupId = "reactive-consumer-group")
    public void consume(String message) {
        log.info("Received message: {}", message);

        try {
            Product product = objectMapper.readValue(message, Product.class);
            productRepository.save(product)
                    .doOnSuccess(saved -> log.info("Product saved: {}", saved))
                    .doOnError(error -> log.error("Error saving product", error))
                    .subscribe();
        } catch (Exception e) {
            log.error("Error processing message", e);
        }
    }
}
