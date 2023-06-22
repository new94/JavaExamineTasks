package ru.mai.examine.rpks;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Slf4j
class Task4Test extends Infrastructure {

    @BeforeEach
    void setUp() {
        super.setUp();
    }

    @AfterEach
    void tearDown() {
        super.tearDown();
    }

    @Test
    void exampleTest() throws Exception {
        this.mockMvc.perform(get("/example?bootstrapServers=" + kafka.getBootstrapServers()
                        + "&redisHost=" + redis.getHost()
                        + "&redisPort=" + redis.getFirstMappedPort()
                        + "&mongoConnectionString=" + mongoDBContainer.getConnectionString()
                        + "&mongoDatabase=mongo_test_db&mongoCollection=mongo_test_collection"))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().string("kafka:" + kafka.getBootstrapServers() + ";\n" +
                        "redis:" + redis.getHost() + "," + redis.getFirstMappedPort() + ";\n" +
                        "mongo:" + mongoDBContainer.getConnectionString() + ",mongo_test_db,mongo_test_collection;\n"));
    }

    /**
     * Реализовать endpoint в TaskController, которому при запросе передаётся boostrapServers и название топика.
     * В endpoint создаётся consumer Kafka, который читает топик, начиная с самых старых сообщений.
     * Далее в PostgreSQL находим все строки, которые содержат сообщение Kafka ("test") и
     * возвращаем в endpoint значение с самым большим id из PostgreSQL
     *
     * Как работать с параметрами запроса можно посмотреть в TaskController в endpoint example.
     *
     * Проверить, что инфраструктура работает можно запустив тест exampleTest, если он пройден, то инфраструктура работает
     */
    @Test
    void task4() throws Exception {
        this.mockMvc.perform(get("/task4?bootstrapServers=" + kafka.getBootstrapServers() + "&topic=test_topic_in"))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().string("test value 10"));
    }

}