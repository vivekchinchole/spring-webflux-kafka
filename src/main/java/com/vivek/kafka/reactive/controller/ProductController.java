package com.vivek.kafka.reactive.controller;

import com.vivek.kafka.reactive.entity.Product;
import com.vivek.kafka.reactive.repository.ProductRepository;
import com.vivek.kafka.reactive.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/products")
@RequiredArgsConstructor
public class ProductController {

    private final ProductRepository productRepository;
    private final KafkaProducerService kafkaProducerService;

    @GetMapping
    public Flux<Product> getAllProducts() {
        return productRepository.findAll();
    }

    @GetMapping("/{id}")
    public Mono<Product> getProductById(@PathVariable Long id) {
        return productRepository.findById(id);
    }

    @GetMapping("/search")
    public Flux<Product> searchProducts(@RequestParam String name) {
        return productRepository.findByNameContaining(name);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Product> createProduct(@RequestBody Product product) {
        return productRepository.save(product)
                .flatMap(saved ->
                        kafkaProducerService.sendMessage(
                                String.valueOf(saved.getId()), saved
                        ).thenReturn(saved)
                );
    }

    @PutMapping("/{id}")
    public Mono<Product> updateProduct(@PathVariable Long id,
                                       @RequestBody Product product) {
        return productRepository.findById(id)
                .flatMap(existing -> {
                    product.setId(id);
                    return productRepository.save(product);
                })
                .flatMap(updated ->
                        kafkaProducerService.sendMessage(
                                String.valueOf(updated.getId()), updated
                        ).thenReturn(updated)
                );
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> deleteProduct(@PathVariable Long id) {
        return productRepository.deleteById(id);
    }
}
