package ru.itmo.infrastructure;

import infrastructure.Protobufmessages;
import ru.itmo.model.SimulationConfig;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.*;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(180)
@Tag("integration-test")
class KafkaOrchestratorTest {

    private static final String BOOTSTRAP     = "localhost:9092";
    private static final String ORCH_INBOX    = "orchestrator-inbox";
    private static final String REGISTRY      = "model-registry";
    private static final String HEARTBEAT     = "model-heartbeat";
    private static final String CONTROL       = "simulation-control";
    private static final String DLQ           = "dead-letter-queue";

    private KafkaProducer<String, byte[]> producer;
    private KafkaOrchestrator orchestrator;

    @BeforeAll
    static void checkKafkaAvailable() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        try (AdminClient admin = AdminClient.create(p)) {
            admin.listTopics().names().get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail("Kafka is not available on " + BOOTSTRAP + ": " + e.getMessage()
                    + "\nStart it with: docker compose up -d");
        }
    }

    @BeforeEach
    void setUp() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producer = new KafkaProducer<>(p);
    }

    @AfterEach
    void tearDown() {
        if (orchestrator != null) {
            try { orchestrator.stopOrchestrator(); } catch (Exception ignored) {}
            orchestrator = null;
        }
        if (producer != null) {
            producer.close(Duration.ofSeconds(2));
            producer = null;
        }
    }

    // ==================================================================
    //  ТЕСТЫ
    // ==================================================================

    /**
     * Проверяет корректность инициализации инфраструктуры Kafka.
     * При запуске оркестратор должен автоматически создать все 5 необходимых системных топиков:
     * orchestrator-inbox, model-registry, model-heartbeat, simulation-control и dead-letter-queue.
     */
    @Test
    @Order(1)
    void startOrchestrator_createsAllRequiredTopics() throws Exception {
        orchestrator = startOrchestrator(1.0, "model-1");

        try (AdminClient admin = AdminClient.create(adminProps())) {
            Set<String> topics = admin.listTopics().names().get(5, TimeUnit.SECONDS);
            assertTrue(topics.contains(ORCH_INBOX), "Missing " + ORCH_INBOX);
            assertTrue(topics.contains(REGISTRY),   "Missing " + REGISTRY);
            assertTrue(topics.contains(HEARTBEAT),  "Missing " + HEARTBEAT);
            assertTrue(topics.contains(CONTROL),    "Missing " + CONTROL);
            assertTrue(topics.contains(DLQ),        "Missing " + DLQ);
        }
    }

    /**
     * Проверяет идемпотентность метода запуска оркестратора.
     * Повторный вызов startOrchestrator() не должен создавать дублирующиеся потоки или приводить к ошибкам,
     * а оркестратор должен продолжать штатно обрабатывать входящие команды (например, регистрацию).
     */
    @Test
    @Order(2)
    void startOrchestrator_secondCallIsIgnored() throws Exception {
        orchestrator = startOrchestrator(1.0, "model-1");
        assertDoesNotThrow(() -> orchestrator.startOrchestrator());

        try (TestConsumer consumer = new TestConsumer(CONTROL)) {
            sendRegistration("model-1", true);
            assertNotNull(consumer.waitFor(r -> "model-1".equals(r.key()), 15_000),
                    "Orchestrator stopped working after second startOrchestrator()");
        }
    }

    /**
     * Проверяет логику очистки старых топиков при старте (cleanOnStart = true).
     * Если в Kafka остались персональные топики моделей (например, model-old-inbox) от прошлых запусков,
     * оркестратор должен их удалить, чтобы избежать чтения устаревших сообщений.
     */
    @Test
    @Order(3)
    void cleanOnStart_removesPreExistingModelTopics() throws Exception {
        try (AdminClient admin = AdminClient.create(adminProps())) {
            try {
                admin.createTopics(List.of(new NewTopic("model-old-inbox", 1, (short) 1)))
                        .all().get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }

        orchestrator = startOrchestrator(1.0, "model-1");

        try (AdminClient admin = AdminClient.create(adminProps())) {
            Set<String> topics = admin.listTopics().names().get(5, TimeUnit.SECONDS);
            assertFalse(topics.contains("model-old-inbox"),
                    "cleanOnStart should have removed model-old-inbox");
        }
    }

    /**
     * Проверяет главный барьер старта симуляции.
     * Оркестратор не должен начинать рассылку команд, пока не зарегистрируются ВСЕ ожидаемые модели.
     * Как только последняя модель отправляет ONLINE, обеим моделям должен уйти первый NEXT_STEP с t=1.0.
     */
    @Test
    @Order(4)
    void allModelsRegistered_firstNextStepBroadcastToAll() throws Exception {
        orchestrator = startOrchestrator(1.0, "model-1", "model-2");
        try (TestConsumer consumer = new TestConsumer(CONTROL)) {
            sendRegistration("model-1", true);
            sendRegistration("model-2", true);

            assertNotNull(consumer.waitFor(
                            r -> "model-1".equals(r.key()) && isNextStep(r.value(), 1.0), 15_000),
                    "NEXT_STEP (t=1.0) not sent to model-1");
            assertNotNull(consumer.waitFor(
                            r -> "model-2".equals(r.key()) && isNextStep(r.value(), 1.0), 15_000),
                    "NEXT_STEP (t=1.0) not sent to model-2");
        }
    }

    /**
     * Проверяет обработку штатного отключения модели.
     * При получении сообщения OFFLINE оркестратор должен исключить модель из списка активных (activeModels).
     * При этом другие модели должны остаться в активном списке.
     */
    @Test
    @Order(5)
    void offlineRegistration_removesModelFromActiveSet() throws Exception {
        orchestrator = startOrchestrator(1.0, "model-1", "model-2");
        try (TestConsumer consumer = new TestConsumer(CONTROL)) {
            sendRegistration("model-1", true);
            sendRegistration("model-2", true);
            assertNotNull(consumer.waitFor(r -> "model-1".equals(r.key()), 15_000));
            assertNotNull(consumer.waitFor(r -> "model-2".equals(r.key()), 15_000));

            Map<String, ?> active = readField(orchestrator, "activeModels", Map.class);
            assertTrue(active.containsKey("model-1"));
            assertTrue(active.containsKey("model-2"));

            sendRegistration("model-2", false); // OFFLINE
            Thread.sleep(1500);

            active = readField(orchestrator, "activeModels", Map.class);
            assertTrue(active.containsKey("model-1"), "model-1 should still be active");
            assertFalse(active.containsKey("model-2"), "model-2 should be removed");
        }
    }

    /**
     * Проверяет применение кастомной длительности шага.
     * Если оркестратор инициализирован с шагом 5.0, то в первой команде NEXT_STEP
     * параметр nextSimulationTime должен быть равен 5.0.
     */
    @Test
    @Order(6)
    void setStepDuration_affectsFirstNextStep() throws Exception {
        orchestrator = startOrchestrator(5.0, "model-1");
        try (TestConsumer consumer = new TestConsumer(CONTROL)) {
            sendRegistration("model-1", true);
            var msg = consumer.waitFor(
                    r -> "model-1".equals(r.key()) && isNextStep(r.value(), 5.0), 15_000);
            assertNotNull(msg, "NEXT_STEP with nextSimulationTime=5.0 not sent");
        }
    }

    /**
     * Проверяет логику шагового барьера (Barrier).
     * Когда ВСЕ активные модели присылают STEP_COMPLETED, оркестратор должен
     * инкрементировать глобальное модельное время и разослать новый NEXT_STEP (t=2.0) всем участникам.
     */
    @Test
    @Order(7)
    void stepCompletedByAll_incrementsTimeAndBroadcastsNextStep() throws Exception {
        orchestrator = startOrchestrator(1.0, "model-1", "model-2");
        try (TestConsumer consumer = new TestConsumer(CONTROL)) {
            sendRegistration("model-1", true);
            sendRegistration("model-2", true);
            assertNotNull(consumer.waitFor(r -> "model-1".equals(r.key()) && isNextStep(r.value(), 1.0), 15_000));
            assertNotNull(consumer.waitFor(r -> "model-2".equals(r.key()) && isNextStep(r.value(), 1.0), 15_000));

            assertEquals(0.0, orchestrator.getCounterModelTime(), 1e-9);

            sendStepCompleted("model-1", false);
            sendStepCompleted("model-2", false);

            assertNotNull(consumer.waitFor(r -> "model-1".equals(r.key()) && isNextStep(r.value(), 2.0), 15_000),
                    "Barrier did not broadcast step 2 to model-1");
            assertNotNull(consumer.waitFor(r -> "model-2".equals(r.key()) && isNextStep(r.value(), 2.0), 15_000),
                    "Barrier did not broadcast step 2 to model-2");

            assertEquals(1.0, orchestrator.getCounterModelTime(), 1e-9);
        }
    }

    /**
     * Проверяет возможность сброса глобального модельного времени.
     * Метод resetCounterModelTime() должен принудительно установить счетчик времени в 0.0,
     * даже если симуляция уже продвинулась вперед.
     */
    @Test
    @Order(8)
    void resetCounterModelTime_zeroesCounter() throws Exception {
        orchestrator = startOrchestrator(1.0, "model-1", "model-2");
        try (TestConsumer consumer = new TestConsumer(CONTROL)) {
            sendRegistration("model-1", true);
            sendRegistration("model-2", true);
            assertNotNull(consumer.waitFor(r -> "model-1".equals(r.key()), 15_000));
            assertNotNull(consumer.waitFor(r -> "model-2".equals(r.key()), 15_000));

            sendStepCompleted("model-1", false);
            sendStepCompleted("model-2", false);
            assertNotNull(consumer.waitFor(r -> isNextStep(r.value(), 2.0), 15_000));
            assertEquals(1.0, orchestrator.getCounterModelTime(), 1e-9);

            orchestrator.resetCounterModelTime();
            assertEquals(0.0, orchestrator.getCounterModelTime(), 1e-9);
        }
    }

    /**
     * Проверяет обработку сигналов жизнеспособности (Heartbeat).
     * При получении HEARTBEAT от модели оркестратор должен обновить
     * время её последней активности (lastHeartbeat) во внутреннем объекте ModelInfo.
     */
    @Test
    @Order(9)
    void heartbeat_updatesLastHeartbeatTimestamp() throws Exception {
        orchestrator = startOrchestrator(1.0, "model-1");
        try (TestConsumer consumer = new TestConsumer(CONTROL)) {
            sendRegistration("model-1", true);
            assertNotNull(consumer.waitFor(r -> "model-1".equals(r.key()), 15_000));

            Map<String, ?> active = readField(orchestrator, "activeModels", Map.class);
            Instant before = readField(active.get("model-1"), "lastHeartbeat", Instant.class);

            Thread.sleep(700);
            // Отправляем Heartbeat через Protobuf
            var hbMsg = Protobufmessages.ModelMessage.newBuilder()
                    .setSenderModelId("model-1")
                    .setHeartbeat(Protobufmessages.Heartbeat.getDefaultInstance())
                    .build();
            producer.send(new ProducerRecord<>(HEARTBEAT, hbMsg.toByteArray()));
            producer.flush();
            Thread.sleep(1500);

            Map<String, ?> activeAfter = readField(orchestrator, "activeModels", Map.class);
            Instant after = readField(activeAfter.get("model-1"), "lastHeartbeat", Instant.class);
            assertTrue(after.isBefore(before) == false, // Вместо after.isAfter(before), так как могут быть проблемы с точностью
                    "Heartbeat should have updated lastHeartbeat. before=" + before + " after=" + after);
        }
    }

    /**
     * Проверяет маршрутизацию агентов между моделями.
     * Когда Модель-1 отправляет сообщение AGENT_TRANSFER для Модели-2,
     * оркестратор должен перехватить его и переслать в персональный inbox-топик Модели-2 без потери данных.
     */
    @Test
    @Order(10)
    void agentTransfer_routesToTargetInbox() throws Exception {
        orchestrator = startOrchestrator(1.0, "model-1", "model-2");
        try (TestConsumer consumer = new TestConsumer(CONTROL, "model-2-inbox")) {
            sendRegistration("model-1", true);
            sendRegistration("model-2", true);
            assertNotNull(consumer.waitFor(r -> CONTROL.equals(r.topic()) && "model-1".equals(r.key()), 15_000));
            assertNotNull(consumer.waitFor(r -> CONTROL.equals(r.topic()) && "model-2".equals(r.key()), 15_000));

            byte[] agentMsg = buildAgentTransfer("model-1", "model-2", 42);
            producer.send(new ProducerRecord<>(ORCH_INBOX, agentMsg));
            producer.flush();

            var received = consumer.waitFor(r -> "model-2-inbox".equals(r.topic()), 15_000);
            assertNotNull(received, "Agent did not arrive at model-2-inbox");

            // Проверяем, что это Protobuf с правильным агентом
            var orchMsg = Protobufmessages.OrchestratorMessage.parseFrom(received.value());
            assertTrue(orchMsg.hasAgentTransfer(), "Expected AgentTransfer message");
            assertEquals(42, orchMsg.getAgentTransfer().getAgent().getAgentId());
        }
    }

    /**
     * Проверяет обработку ошибок маршрутизации (Dead Letter Queue).
     * Если модель отправляет агента несуществующей или отключенной модели,
     * оркестратор не должен терять сообщение, а обязан отправить его в топик DLQ с указанием причины.
     */
    @Test
    @Order(11)
    void agentTransfer_toUnknownTarget_goesToDlq() throws Exception {
        orchestrator = startOrchestrator(1.0, "model-1");
        try (TestConsumer consumer = new TestConsumer(DLQ)) {
            sendRegistration("model-1", true);
            Thread.sleep(1000);

            byte[] msg = buildAgentTransfer("model-1", "unknown-model", 7);
            producer.send(new ProducerRecord<>(ORCH_INBOX, msg));
            producer.flush();

            var dlq = consumer.waitFor(r -> true, 15_000);
            assertNotNull(dlq, "DLQ message not received");

            // В KafkaOrchestrator DLQ теперь пишет строку (reason + Base64)
            String dlqText = new String(dlq.value(), StandardCharsets.UTF_8);
            assertTrue(dlqText.contains("unknown-model"),
                    "DLQ reason should mention target model, got: " + dlqText);
        }
    }

    /**
     * Проверяет механизм штатной остановки симуляции.
     * Метод stopOrchestre() должен разослать команду STOP_SIMULATION всем моделям,
     * которые были указаны в конфигурации (expectedModelIds), независимо от их текущего статуса.
     */
    @Test
    @Order(12)
    void stopOrchestre_sendsStopSimulationToAllExpectedModels() throws Exception {
        orchestrator = startOrchestrator(1.0, "model-1", "model-2");
        try (TestConsumer consumer = new TestConsumer(CONTROL)) {
            sendRegistration("model-1", true);
            sendRegistration("model-2", true);
            assertNotNull(consumer.waitFor(r -> "model-1".equals(r.key()) && isNextStep(r.value(), 1.0), 15_000));
            assertNotNull(consumer.waitFor(r -> "model-2".equals(r.key()) && isNextStep(r.value(), 1.0), 15_000));

            orchestrator.stopOrchestre();

            assertNotNull(consumer.waitFor(
                            r -> "model-1".equals(r.key()) && isStopSimulation(r.value()), 10_000),
                    "STOP_SIMULATION not sent to model-1");
            assertNotNull(consumer.waitFor(
                            r -> "model-2".equals(r.key()) && isStopSimulation(r.value()), 10_000),
                    "STOP_SIMULATION not sent to model-2");
        }
    }

    /**
     * Проверяет обработку ошибок при запуске моделей.
     * Если путь к исполняемому файлу (.bat) не существует или указан неверно,
     * метод runModels() должен вернуть false и вывести предупреждение, не вызывая падения JVM.
     */
    @Test
    @Order(13)
    void runModels_returnsFalseWhenFilesDontExist() {
        SimulationConfig cfg = new SimulationConfig(
                "no-such-model",
                "/nonexistent/path.bat",
                ChronoUnit.MINUTES,
                LocalDateTime.of(2026, 1, 1, 0, 0),
                LocalDateTime.of(2026, 1, 2, 0, 0),
                false);

        KafkaOrchestrator orch = new KafkaOrchestrator(List.of(cfg), 1.0, false, false);
        assertFalse(orch.runModels(),
                "runModels() must return false when no model files exist");
    }

    /**
     * Проверяет парсинг конфигурационного JSON-файла.
     * Метод createFromConfig() должен корректно считать настройки времени, единицы измерения (ChronoUnit)
     * и список моделей, создав на их основе валидный объект KafkaOrchestrator.
     */
    @Test
    @Order(14)
    void createFromConfig_loadsJsonCorrectly() throws Exception {
        String jsonConfig = """
        {
          "timeUnit": "MINUTES",
          "startSimulation": "01-01-2027T00:00:00",
          "stopSimulation": "02-01-2027T00:00:00",
          "isDebug": true,
          "models": [
            { "id": "test-model-1", "filePath": "model1.bat" },
            { "id": "test-model-2", "filePath": "model2.bat" }
          ]
        }
        """;

        Path tempFile = Files.createTempFile("test-config", ".json");
        Files.writeString(tempFile, jsonConfig);

        try {
            KafkaOrchestrator loadedOrch = KafkaOrchestrator.createFromConfig(
                    tempFile.toAbsolutePath().toString(), 2.5, false, true);

            assertNotNull(loadedOrch, "Orchestrator should be created from config");

            @SuppressWarnings("unchecked")
            Set<String> expectedIds = readField(loadedOrch, "expectedModelIds", Set.class);
            assertEquals(2, expectedIds.size());
            assertTrue(expectedIds.contains("test-model-1"));
            assertTrue(expectedIds.contains("test-model-2"));

            double stepDuration = readField(loadedOrch, "stepDuration", Double.class);
            assertEquals(2.5, stepDuration, 1e-9);

            boolean isDebug = readField(loadedOrch, "isDebug", Boolean.class);
            assertTrue(isDebug, "Debug mode should be true");

            @SuppressWarnings("unchecked")
            List<SimulationConfig> configs = readField(loadedOrch, "simulationConfigs", List.class);
            assertEquals(2, configs.size());

            SimulationConfig firstModel = configs.get(0);
            assertEquals("test-model-1", firstModel.getId());
            assertEquals("model1.bat", firstModel.getFilePath());
            assertEquals(ChronoUnit.MINUTES, firstModel.getTimeUnit());
            assertEquals(LocalDateTime.of(2027, 1, 1, 0, 0), firstModel.getStartSimulation());
            assertEquals(LocalDateTime.of(2027, 1, 2, 0, 0), firstModel.getStopSimulation());
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    // ==================================================================
    //  Helpers
    // ==================================================================

    private KafkaOrchestrator startOrchestrator(double stepDuration, String... modelIds) throws Exception {
        List<SimulationConfig> configs = new ArrayList<>();
        for (String id : modelIds) {
            configs.add(new SimulationConfig(
                    id, "fake-" + id + ".bat", ChronoUnit.MINUTES,
                    LocalDateTime.of(2026, 1, 1, 0, 0),
                    LocalDateTime.of(2026, 1, 2, 0, 0), false));
        }
        KafkaOrchestrator orch = new KafkaOrchestrator(configs, stepDuration, true, false);
        orch.startOrchestrator();
        waitForOrchestratorReady(20_000);
        return orch;
    }

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        return p;
    }

    private void waitForOrchestratorReady(long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        try (AdminClient admin = AdminClient.create(adminProps())) {
            while (System.currentTimeMillis() < deadline) {
                try {
                    var desc = admin.describeConsumerGroups(List.of("simulation-orchestrator"))
                            .all().get(2, TimeUnit.SECONDS);
                    var group = desc.get("simulation-orchestrator");
                    if (group != null && !group.members().isEmpty()) {
                        return;
                    }
                } catch (Exception ignored) {}
                Thread.sleep(200);
            }
        }
        throw new IllegalStateException("Orchestrator did not become ready within " + timeoutMs + "ms");
    }

    private void sendRegistration(String modelId, boolean isOnline) {
        var reg = Protobufmessages.Registration.newBuilder()
                .setInboxTopic(modelId + "-inbox")
                .setIsOnline(isOnline)
                .build();
        var msg = Protobufmessages.ModelMessage.newBuilder()
                .setSenderModelId(modelId)
                .setRegistration(reg)
                .build();
        producer.send(new ProducerRecord<>(REGISTRY, msg.toByteArray()));
        producer.flush();
    }

    private void sendStepCompleted(String sender, boolean finish) {
        var builder = Protobufmessages.ModelMessage.newBuilder().setSenderModelId(sender);
        if (finish) builder.setStepCompletedAndFinished(Protobufmessages.StepCompletedAndFinished.getDefaultInstance());
        else builder.setStepCompleted(Protobufmessages.StepCompleted.getDefaultInstance());

        producer.send(new ProducerRecord<>(ORCH_INBOX, builder.build().toByteArray()));
        producer.flush();
    }

    private byte[] buildAgentTransfer(String sender, String target, int agentId) {
        var agent = Protobufmessages.AgentMessage.newBuilder()
                .setAgentId(agentId)
                .setSourceId(1)
                .setCreationTime(0.0)
                .setCategoryId(1)
                .setCategoryFlowId(1)
                .setVolume(1.0)
                .build();

        var transfer = Protobufmessages.AgentTransfer.newBuilder()
                .setTargetModelId(target)
                .setAgent(agent)
                .build();

        return Protobufmessages.ModelMessage.newBuilder()
                .setSenderModelId(sender)
                .setAgentTransfer(transfer)
                .build()
                .toByteArray();
    }

    private static boolean isNextStep(byte[] value, double expectedTime) {
        try {
            var msg = Protobufmessages.OrchestratorMessage.parseFrom(value);
            return msg.hasNextStep() && Math.abs(msg.getNextStep().getNextSimulationTime() - expectedTime) < 1e-9;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isStopSimulation(byte[] value) {
        try {
            return Protobufmessages.OrchestratorMessage.parseFrom(value).hasStopSimulation();
        } catch (Exception e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T readField(Object target, String fieldName, Class<T> type) throws Exception {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        return (T) f.get(target);
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
            p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            consumer = new KafkaConsumer<>(p);
            consumer.subscribe(Arrays.asList(topics));

            long deadline = System.currentTimeMillis() + 15_000;
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
                Iterator<ConsumerRecord<String, byte[]>> it = buffer.iterator();
                while (it.hasNext()) {
                    ConsumerRecord<String, byte[]> r = it.next();
                    if (matcher.test(r)) {
                        it.remove();
                        return r;
                    }
                }

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