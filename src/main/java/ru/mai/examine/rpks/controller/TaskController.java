package ru.mai.examine.rpks.controller;

import io.swagger.v3.oas.annotations.Operation;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

@RestController
@Slf4j
public class TaskController {

    @GetMapping("/example")
    @ResponseStatus(value = HttpStatus.OK)
    @Operation(summary = "example")
    public String example(@RequestParam String bootstrapServers, @RequestParam String redisHost,
                          @RequestParam Integer redisPort, @RequestParam String mongoConnectionString,
                          @RequestParam String mongoDatabase, @RequestParam String mongoCollection) {
        return "kafka:" + bootstrapServers + ";\n" +
                "redis:" + redisHost + "," + redisPort + ";\n" +
                "mongo:" + mongoConnectionString + "," + mongoDatabase + "," + mongoCollection + ";\n";
    }
}
