package ru.itmo.infrastructure;

import infrastructure.Protobufmessages;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration-test")
@Timeout(60)
class ConsumerOrchestrantServiceTest extends AbstractKafkaIntegrationTest {

    private static final String CONTROL_TOPIC = "simulation-control";
    private static final String MODEL_ID = "test-consumer-model";

    private KafkaOrchestrant owner;
    private ConsumerOrchestrantService service;
    private KafkaProducer<String, byte[]> testProducer;
    private KafkaProducer<String, byte[]> ownerProducer;  // <-- новое поле
    private TestEngine testEngine;

    private final AtomicReference<byte[]> capturedAgentData = new AtomicReference<>();

    @BeforeEach
    void setUp() throws Exception {
        testEngine = new TestEngine(0.0, 100.0, 50);
        owner = new KafkaOrchestrant(MODEL_ID, testEngine, data -> capturedAgentData.set(data), true);

        // ==========================================
        // ВАЖНО: создаём настоящий producer и внедряем его в owner.
        // Без этого owner.sendStepCompleted() упадёт с NPE,
        // потому что owner.init() (который создаёт producer) не вызывается.
        // ==========================================
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Таймауты, чтобы продюсер не висел бесконечно при проблемах с брокером
        prodProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        prodProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        prodProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);

        ownerProducer = new KafkaProducer<>(prodProps);
        Field producerField = KafkaOrchestrant.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(owner, ownerProducer);

        // Включаем флаг работы
        Field runningField = KafkaOrchestrant.class.getDeclaredField("running");
        runningField.setAccessible(true);
        ((AtomicBoolean) runningField.get(owner)).set(true);

        // Готовим модель к приёму шага (эмуляция FirstStepEvent)
        owner.setReadyForStep(true);
        owner.setTrueFirstStepFinished();

        service = new ConsumerOrchestrantService(owner);
        service.start();

        // Producer для отправки команд в топики
        testProducer = new KafkaProducer<>(prodProps);

        Thread.sleep(2000); // Ждём назначения партиций
    }

    @AfterEach
    void tearDown() throws Exception {
        if (service != null) service.stop();

        Field runningField = KafkaOrchestrant.class.getDeclaredField("running");
        runningField.setAccessible(true);
        ((AtomicBoolean) runningField.get(owner)).set(false);

        if (testProducer != null) testProducer.close(Duration.ofSeconds(2));
        if (ownerProducer != null) ownerProducer.close(Duration.ofSeconds(2));
    }

    @Test
    void commandThread_processesNextStepForOwnId() throws Exception {
        var msg = Protobufmessages.OrchestratorMessage.newBuilder()
                .setNextStep(Protobufmessages.NextStepCommand.newBuilder().setNextSimulationTime(10.0))
                .build();
        testProducer.send(new ProducerRecord<>(CONTROL_TOPIC, MODEL_ID, msg.toByteArray()));
        testProducer.flush();

        // Ждём, пока движок выполнит шаг
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline && testEngine.getRunFastCount() == 0) {
            Thread.sleep(100);
        }

        assertTrue(testEngine.getRunFastCount() > 0, "engine.runFast() should be called");
        assertEquals(1, testEngine.getStepEndEventCount(), "StepEndEvent should be executed once");
        assertEquals(10.0, testEngine.getLastStepEndDuration(), 1e-9, "Duration should be 10.0");
    }

    @Test
    void commandThread_ignoresCommandsForOtherModels() throws Exception {
        var msg = Protobufmessages.OrchestratorMessage.newBuilder()
                .setNextStep(Protobufmessages.NextStepCommand.newBuilder().setNextSimulationTime(10.0))
                .build();
        testProducer.send(new ProducerRecord<>(CONTROL_TOPIC, "other-model", msg.toByteArray()));
        testProducer.flush();

        Thread.sleep(2000);

        assertEquals(0, testEngine.getRunFastCount(), "engine.runFast() should NOT be called for other model");
        assertEquals(0, testEngine.getStepEndEventCount(), "StepEndEvent should NOT be executed");
    }

    @Test
    void consumerThread_receivesAgentAndProcessesOnNextStep() throws Exception {
        var agent = Protobufmessages.AgentMessage.newBuilder().setAgentId(999).build();
        var transfer = Protobufmessages.AgentTransfer.newBuilder().setAgent(agent).build();
        var agentMsg = Protobufmessages.OrchestratorMessage.newBuilder().setAgentTransfer(transfer).build();

        testProducer.send(new ProducerRecord<>(MODEL_ID + "-inbox", agentMsg.toByteArray()));
        testProducer.flush();

        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline && owner.tasksFromKafka.isEmpty()) {
            Thread.sleep(100);
        }
        assertEquals(1, owner.tasksFromKafka.size(), "Agent should be queued in tasksFromKafka");

        var stepMsg = Protobufmessages.OrchestratorMessage.newBuilder()
                .setNextStep(Protobufmessages.NextStepCommand.newBuilder().setNextSimulationTime(10.0))
                .build();
        testProducer.send(new ProducerRecord<>(CONTROL_TOPIC, MODEL_ID, stepMsg.toByteArray()));
        testProducer.flush();

        deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline && capturedAgentData.get() == null) {
            Thread.sleep(100);
        }

        assertNotNull(capturedAgentData.get(), "receivingAgent should be called");
        var receivedAgent = Protobufmessages.AgentMessage.parseFrom(capturedAgentData.get());
        assertEquals(999, receivedAgent.getAgentId());
    }
}