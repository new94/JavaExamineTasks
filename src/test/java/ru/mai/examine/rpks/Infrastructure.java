package ru.mai.examine.rpks;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Sorts;
import com.redis.testcontainers.RedisContainer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.Document;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.JedisPooled;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.eq;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@AutoConfigureMockMvc
@Slf4j
@Testcontainers
@ContextConfiguration(initializers = {Infrastructure.Initializer.class})
class Infrastructure {
    static int containerPort = 5432;
    static int localPort = 5432;

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"));

    @Container
    static RedisContainer redis = new RedisContainer(DockerImageName.parse("redis:5.0.3-alpine"))
            .withExposedPorts(6379);

    @Container
    static MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:4.0.10"));

    @Container
    static PostgreSQLContainer<?> postgreSQL = new PostgreSQLContainer<>(DockerImageName.parse("postgres"))
            .withDatabaseName("test_db")
            .withUsername("user")
            .withPassword("password")
            .withInitScript("init_script.sql")
            .withReuse(true)
            .withExposedPorts(containerPort)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                    new HostConfig().withPortBindings(new PortBinding(Ports.Binding.bindPort(localPort), new ExposedPort(containerPort)))
            ));

    static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    "spring.datasource.url=" + postgreSQL.getJdbcUrl(),
                    "spring.datasource.username=" + postgreSQL.getUsername(),
                    "spring.datasource.password=" + postgreSQL.getPassword()
            ).applyTo(configurableApplicationContext.getEnvironment());
        }
    }

    protected String tableName = "test_table";

    protected DataSource dataSource;

    @Autowired
    protected MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        dataSource = Optional.ofNullable(dataSource).orElse(createConnectionPool());
        createTopics();
        var listDataIn = List.of(
                "test",
                "number 1", "number 2", "number 3",
                "number 4", "number 5", "number 6",
                "number 7", "number 8", "number 9"
        );
        listDataIn.forEach(data -> sendMessagesToTestTopic(createProducer(), data));

        JedisPooled jedisPooled = createRedisClient();
        String testKey = "test";
        String expectedValue = "test";
        jedisPooled.set(testKey, expectedValue);

        Document document = new Document().append("test_value_field", "test_value");

        createAndCheckDocumentInMongoDB(document, "test_value_field", "test_value");
    }

    @AfterEach
    void tearDown() {
        clearTable();
    }

    protected HikariDataSource createConnectionPool() {

        log.info("Create connection pool");
        var config = new HikariConfig();
        config.setJdbcUrl(postgreSQL.getJdbcUrl());
        config.setUsername(postgreSQL.getUsername());
        config.setPassword(postgreSQL.getPassword());
        config.setDriverClassName(postgreSQL.getDriverClassName());
        return new HikariDataSource(config);
    }

    private void createAndCheckDocumentInMongoDB(Document document, String conditionField, String conditionValue) {
        var mongoClient = getMongoClient();
        log.info("Create and check document in MongoDB by condition {} = {}", conditionField, conditionValue);
        var mongoCollection = mongoClient.getDatabase("mongo_test_db").getCollection("mongo_test_collection");
        mongoCollection.insertOne(document);
        var actualDocument = Optional.ofNullable(mongoCollection
                .find(eq(conditionField, conditionValue))
                .sort(Sorts.descending("_id"))
                .first());
        assertFalse(actualDocument.isEmpty());
        assertEquals(document, actualDocument.get());

        log.info("Documents in MongoDB:");
        mongoCollection
                .find(eq(conditionField, conditionValue))
                .forEach(d -> log.info("Document: {}", d));

    }

    private MongoClient getMongoClient() {
        log.info("Create mongo client");
        return MongoClients.create(mongoDBContainer.getConnectionString());
    }

    private JedisPooled createRedisClient() {
        return new JedisPooled(redis.getHost(), redis.getFirstMappedPort());
    }

    private void createTopics() {
        var adminClient = createAdminClient();
        List<NewTopic> topics = Stream.of("test_topic_in")
                .map(topicName -> new NewTopic(topicName, 3, (short) 1))
                .toList();

        checkAndCreateRequiredTopics(adminClient, topics);
    }

    private void sendMessagesToTestTopic(Producer<String, String> producer, String data) {
        log.info("Send message to kafka {}", data);
        try {
            producer.send(new ProducerRecord<>("test_topic_in", "expected", data)).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error send message to kafka topic", e);
            fail();
        }
    }

    private AdminClient createAdminClient() {
        log.info("Create admin client");
        return AdminClient.create(ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
    }

    private void checkAndCreateRequiredTopics(Admin adminClient, List<NewTopic> topics) {
        log.info("Check required topics");
        try {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            if (existingTopics.isEmpty()) {
                log.info("Topic not exist. Create topics {}", topics);
                adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
            } else {
                topics.stream().map(NewTopic::name).filter(t -> !existingTopics.contains(t)).forEach(t -> {
                    try {
                        log.info("Topic not exist {}. Create topic {}", t, t);
                        adminClient.createTopics(List.of(new NewTopic(t, 3, (short) 1))).all().get(30, TimeUnit.SECONDS);
                    } catch (InterruptedException | TimeoutException | ExecutionException e) {
                        log.error("Error creating topic Kafka", e);
                    }
                });
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Error checking topics", e);
        }
    }

    private KafkaProducer<String, String> createProducer() {
        log.info("Create producer");
        return new KafkaProducer<>(
                ImmutableMap.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );
    }

    protected void clearTable() {
        log.info("Clear table PostgreSQL: {}", tableName);
        try {
            DSLContext context = DSL.using(dataSource.getConnection(), SQLDialect.POSTGRES);
            context.deleteFrom(table(tableName)).execute();

            var result = context.select(
                            field("id"),
                            field("test_value")
                    )
                    .from(table(tableName))
                    .fetch();

            context.alterSequence(tableName + "_id_seq").restart().execute();

            assertTrue(result.isEmpty());
        } catch (SQLException ex) {
            log.error("Error truncate table", ex);
        }
    }
}
