package ru.itmo.infrastructure;

import infrastructure.Protobufmessages;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Интеграционные тесты для RegistrationOrchestrantService.
 * Проверяют отправку сообщений ONLINE и OFFLINE в топик model-registry.
 */
@Tag("integration-test")
@Timeout(30)
class RegistrationOrchestrantServiceTest extends AbstractKafkaIntegrationTest{

    //private static final String BOOTSTRAP = "localhost:9092";
    private static final String REGISTRY_TOPIC = "model-registry";
    private static final String MODEL_ID = "test-registration-model";
    private static final String INBOX_TOPIC = MODEL_ID + "-inbox";

    private KafkaOrchestrant owner;
    private RegistrationOrchestrantService service;
    private KafkaProducer<String, byte[]> testProducer;

    @BeforeEach
    void setUp() throws Exception {
        // 1. Создаем владельца (KafkaOrchestrant)
        owner = new KafkaOrchestrant(MODEL_ID, null, null, false);

        // 2. Создаем настоящий продюсер и внедряем его в owner через рефлексию
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        testProducer = new KafkaProducer<>(p);

        Field producerField = KafkaOrchestrant.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(owner, testProducer);

        // 3. Инициализируем тестируемый сервис
        service = new RegistrationOrchestrantService(owner);
    }

    @AfterEach
    void tearDown() {
        if (testProducer != null) {
            testProducer.close(Duration.ofSeconds(2));
        }
    }

    @Test
    void subscribe_sendsOnlineRegistration() throws Exception {
        // Используем "latest", чтобы не читать старые сообщения с прошлых запусков
        try (TestConsumer consumer = new TestConsumer(REGISTRY_TOPIC)) {
            // Вызываем тестируемый метод
            service.subscribe();

            // Ждем сообщение от нашей модели
            var received = consumer.waitFor(
                    r -> r.key() == null || MODEL_ID.equals(r.key()), 10_000);

            assertNotNull(received, "Registration ONLINE message was not received");

            // Парсим Protobuf и проверяем поля
            var msg = Protobufmessages.ModelMessage.parseFrom(received.value());
            assertEquals(MODEL_ID, msg.getSenderModelId(), "Sender ID mismatch");

            assertTrue(msg.hasRegistration(), "Message must contain Registration");
            var reg = msg.getRegistration();

            assertTrue(reg.getIsOnline(), "Registration status must be ONLINE (true)");
            assertEquals(INBOX_TOPIC, reg.getInboxTopic(), "Inbox topic mismatch");
        }
    }

    @Test
    void unsubscribe_sendsOfflineRegistration() throws Exception {
        try (TestConsumer consumer = new TestConsumer(REGISTRY_TOPIC)) {
            // Вызываем тестируемый метод
            service.unsubscribe();

            // Ждем сообщение от нашей модели
            var received = consumer.waitFor(
                    r -> r.key() == null || MODEL_ID.equals(r.key()), 10_000);

            assertNotNull(received, "Registration OFFLINE message was not received");

            // Парсим Protobuf и проверяем поля
            var msg = Protobufmessages.ModelMessage.parseFrom(received.value());
            assertEquals(MODEL_ID, msg.getSenderModelId(), "Sender ID mismatch");

            assertTrue(msg.hasRegistration(), "Message must contain Registration");
            var reg = msg.getRegistration();

            assertFalse(reg.getIsOnline(), "Registration status must be OFFLINE (false)");
            // Поле inboxTopic при отписке не заполняется (это нормально)
        }
    }

    /**
     * Вспомогательный консьюмер для перехвата сообщений.
     */
    private static class TestConsumer implements AutoCloseable {
        private final KafkaConsumer<String, byte[]> consumer;
        private final List<ConsumerRecord<String, byte[]>> buffer = new ArrayList<>();

        TestConsumer(String topic) {
            Properties p = new Properties();
            p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
            p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-reg-" + UUID.randomUUID());
            p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            // Важно: читаем только новые сообщения
            p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

            consumer = new KafkaConsumer<>(p);
            consumer.subscribe(Collections.singletonList(topic));

            // Ждем назначения партиций
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(200));
                if (!consumer.assignment().isEmpty()) return;
            }
            throw new IllegalStateException("TestConsumer: no partition assignment");
        }

        ConsumerRecord<String, byte[]> waitFor(
                Predicate<ConsumerRecord<String, byte[]>> matcher, long timeoutMs) {

            long deadline = System.currentTimeMillis() + timeoutMs;

            while (System.currentTimeMillis() < deadline) {
                // 1. Проверяем буфер
                Iterator<ConsumerRecord<String, byte[]>> it = buffer.iterator();
                while (it.hasNext()) {
                    ConsumerRecord<String, byte[]> r = it.next();
                    if (matcher.test(r)) {
                        it.remove();
                        return r;
                    }
                }

                // 2. Читаем новые
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, byte[]> r : records) {
                    buffer.add(r);
                }
            }
            return null;
        }

        @Override
        public void close() {
            try { consumer.close(Duration.ofSeconds(2)); } catch (Exception ignored) {}
        }
    }
}