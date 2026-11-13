package ru.itmo.infrastructure;

import infrastructure.Protobufmessages;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration-test")
@Timeout(60)
class KafkaOrchestrantTest {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String REGISTRY = "model-registry";
    private static final String CONTROL = "simulation-control";
    private static final String MODEL_ID = "orch-test-model";

    private KafkaOrchestrant orchestrant;
    private TestEngine engine;
    private KafkaProducer<String, byte[]> testProducer;

    @BeforeEach
    void setUp() {
        engine = new TestEngine(0.0, 100.0, 50);
        orchestrant = new KafkaOrchestrant(MODEL_ID, engine, data -> {}, true);

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        testProducer = new KafkaProducer<>(p);
    }

    @AfterEach
    void tearDown() {
        if (testProducer != null) testProducer.close();
    }

    /**
     * Проверяет штатную регистрацию модели.
     * При вызове init() в топик model-registry должно улететь сообщение REGISTRATION с флагом ONLINE=true,
     * содержащее правильный inboxTopic и senderModelId.
     */
    @Test
    void orchestrant_init_sendsOnlineRegistration() throws Exception {
        try (TestConsumer consumer = new TestConsumer(REGISTRY)) {
            orchestrant.init();

            var received = consumer.waitFor(r -> {
                try {
                    var msg = Protobufmessages.ModelMessage.parseFrom(r.value());
                    return msg.getKindCase() == Protobufmessages.ModelMessage.KindCase.REGISTRATION
                            && MODEL_ID.equals(msg.getSenderModelId());
                } catch (Exception e) { return false; }
            }, 10_000);

            assertNotNull(received, "ONLINE registration message was not received");
            var msg = Protobufmessages.ModelMessage.parseFrom(received.value());
            assertTrue(msg.getRegistration().getIsOnline());
            assertEquals(MODEL_ID + "-inbox", msg.getRegistration().getInboxTopic());
        }
    }

    /**
     * Проверяет штатное отключение модели через stop().
     * При вызове stop() в топик model-registry должно улететь сообщение OFFLINE,
     * чтобы оркестратор корректно исключил модель из ансамбля.
     */
    @Test
    void orchestrant_stop_sendsOfflineRegistration() throws Exception {
        orchestrant.init();

        try (TestConsumer consumer = new TestConsumer(REGISTRY)) {
            orchestrant.stop();

            var received = consumer.waitFor(r -> {
                try {
                    var msg = Protobufmessages.ModelMessage.parseFrom(r.value());
                    return msg.getKindCase() == Protobufmessages.ModelMessage.KindCase.REGISTRATION
                            && MODEL_ID.equals(msg.getSenderModelId());
                } catch (Exception e) { return false; }
            }, 10_000);

            assertNotNull(received, "OFFLINE registration message was not received");
            var msg = Protobufmessages.ModelMessage.parseFrom(received.value());
            assertFalse(msg.getRegistration().getIsOnline(), "isOnline must be false for OFFLINE");
        }
    }

    /**
     * Проверяет нештатное отключение модели через exit().
     * При вызове exit() сообщение OFFLINE в model-registry НЕ отправляется,
     * так как оркестратор должен сам понять, что модель мертва по таймауту Heartbeat.
     */
    @Test
    void orchestrant_exit_doesNotSendOfflineRegistration() throws Exception {
        orchestrant.init();

        try (TestConsumer consumer = new TestConsumer(REGISTRY)) {
            orchestrant.exit();
            Thread.sleep(3000); // Даем время, чтобы сообщение долетело, если оно вдруг есть

            var received = consumer.waitFor(r -> {
                try {
                    var msg = Protobufmessages.ModelMessage.parseFrom(r.value());
                    return msg.getKindCase() == Protobufmessages.ModelMessage.KindCase.REGISTRATION
                            && MODEL_ID.equals(msg.getSenderModelId());
                } catch (Exception e) { return false; }
            }, 2000);

            assertNull(received, "exit() should NOT send OFFLINE registration");
        }
    }

    /**
     * Проверяет обработку команды NEXT_STEP от оркестратора.
     * Когда в топик simulation-control приходит команда на шаг,
     * оркестрант должен передать управление движку (вызвать runFast).
     */
    @Test
    void orchestrant_processesNextStepCommand() throws Exception {
        orchestrant.init();
        Thread.sleep(1000); // Ждем инициализации
        orchestrant.setReadyForStep(true); // Имитируем готовность

        var msg = Protobufmessages.OrchestratorMessage.newBuilder()
                .setNextStep(Protobufmessages.NextStepCommand.newBuilder().setNextSimulationTime(10.0))
                .build();

        testProducer.send(new ProducerRecord<>(CONTROL, MODEL_ID, msg.toByteArray()));
        testProducer.flush();

        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline && engine.getRunFastCount() == 0) {
            Thread.sleep(100);
        }

        assertTrue(engine.getRunFastCount() > 0, "Engine runFast() should be called");
        assertEquals(10.0, engine.getLastStepEndDuration(), 1e-9);
    }

    private static class TestConsumer implements AutoCloseable {
        private final KafkaConsumer<String, byte[]> consumer;
        private final List<ConsumerRecord<String, byte[]>> buffer = new ArrayList<>();

        TestConsumer(String... topics) {
            Properties p = new Properties();
            p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
            p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
            p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // Читаем только новые
            consumer = new KafkaConsumer<>(p);
            consumer.subscribe(Arrays.asList(topics));

            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(200));
                if (!consumer.assignment().isEmpty()) return;
            }
            throw new IllegalStateException("No partition assignment");
        }

        ConsumerRecord<String, byte[]> waitFor(java.util.function.Predicate<ConsumerRecord<String, byte[]>> matcher, long timeoutMs) {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                Iterator<ConsumerRecord<String, byte[]>> it = buffer.iterator();
                while (it.hasNext()) {
                    ConsumerRecord<String, byte[]> r = it.next();
                    if (matcher.test(r)) { it.remove(); return r; }
                }
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, byte[]> r : records) buffer.add(r);
            }
            return null;
        }

        @Override
        public void close() { try { consumer.close(); } catch (Exception ignored) {} }
    }
}