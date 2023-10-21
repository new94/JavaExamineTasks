package ru.mai.examine.rpks.controller;

import io.swagger.v3.oas.annotations.Operation;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import redis.clients.jedis.JedisPooled;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
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
    @GetMapping("/task2")
    public String taskEndpoint(@RequestParam String bootstrapServers, @RequestParam String topic,
                               @RequestParam String redisHost,
                               @RequestParam Integer redisPort) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.GROUP_ID_CONFIG, "0"
                ),
                new StringDeserializer(),
                new StringDeserializer() ))
        {
            consumer.subscribe(Collections.singletonList(topic));
            String value = null;
            while (value == null) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> rec : records) {
                    if (rec.value().contains("test")) {
                        value = rec.value();
                        break;
                    }
                }
            }
            log.info("MESSAGE READ: \t" + value);
            try (JedisPooled jedis = new JedisPooled(redisHost, redisPort)) {
                log.info("Read data from Redis by key: {}", value);
                String str = jedis.get(value);
                StringBuilder res = new StringBuilder();
                for (int index = 0; index < str.length()/2; ++index) {
                    res.append(str.charAt(index*2));
                }
                return res.toString();
            }
        } catch (Exception e) {
            log.error(e.toString());
        }
        return "";
    }
}
