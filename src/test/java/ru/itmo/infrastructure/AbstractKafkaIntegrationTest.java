package ru.itmo.infrastructure;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class AbstractKafkaIntegrationTest {

    @Container
    protected static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:3.8.0")
    );

    @BeforeAll
    static void exposeBootstrapServersAsProperty() {
        String servers = KAFKA.getBootstrapServers();
        System.setProperty("kafka.bootstrap.servers", servers);
        System.out.println("Kafka bootstrap servers: " + servers);
    }

    protected static String getBootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}