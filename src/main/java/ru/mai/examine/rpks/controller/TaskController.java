package ru.mai.examine.rpks.controller;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.swagger.v3.oas.annotations.Operation;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@RestController
@Slf4j
public class TaskController {

    @GetMapping("/example")
    @ResponseStatus(value = HttpStatus.OK)
    @Operation(summary = "example")
    public String example(@RequestParam String bootstrapServers, @RequestParam String redisHost,
                          @RequestParam Integer redisPort, @RequestParam String mongoConnectionString,
                          @RequestParam String mongoDatabase, @RequestParam String mongoCollection,
    @RequestParam String postgreSQLJdbcUrl, @RequestParam String  postgreSQLUsername,
                          @RequestParam String postgreSQLPassword, @RequestParam String postgreSQLDriverClassName) {
        log.info("Read data from Redis by key: {}", postgreSQLJdbcUrl + " " +
                postgreSQLUsername + " " +
                postgreSQLPassword + " " +
                postgreSQLDriverClassName );

        return "kafka:" + bootstrapServers + ";\n" +
                "redis:" + redisHost + "," + redisPort + ";\n" +
                "mongo:" + mongoConnectionString + "," + mongoDatabase + "," + mongoCollection + ";\n";
    }



    @GetMapping("/task1")
    public String taskEndpoint(@RequestParam String bootstrapServers, @RequestParam String topic,
                @RequestParam String postgreSQLJdbcUrl, @RequestParam String  postgreSQLUsername,
                @RequestParam String postgreSQLPassword, @RequestParam String postgreSQLDriverClassName) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.GROUP_ID_CONFIG, "0"
                ),
                new StringDeserializer(),
                new StringDeserializer())) {
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
            log.info("Read data from Redis by key: {}", value);


            log.info("Create connection pool");
            var config = new HikariConfig();
            config.setJdbcUrl(postgreSQLJdbcUrl);
            config.setUsername(postgreSQLUsername);
            config.setPassword(postgreSQLPassword);
            config.setDriverClassName(postgreSQLDriverClassName);
            HikariDataSource ds = new HikariDataSource(config);


            Integer wordCount = 0;
            try (Connection connection = ds.getConnection()) {
                DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);

                var recordsFromDb = context.select(
                                field("test_value")
                        )
                        .from(table("test_table"))
                        .where(field("test_value").like("%" + value + "%"))
                        .fetch();



                for (var record :
                        recordsFromDb) {
                    log.info("Read data from Redis by key: {}", record);
                    log.info("Read data from Redis by key: {}", record.getValue("test_value"));

                    int count = ((String) record.getValue("test_value")).split("\n").length;
                    wordCount += count;
                }
            } catch (Exception e) {
                log.error(e.toString());
            }
            log.info("Read data from Redis by key: {}", wordCount.toString());

            return wordCount.toString();
        }
    }

}
