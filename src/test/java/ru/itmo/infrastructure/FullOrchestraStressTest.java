package ru.itmo.infrastructure;

import ru.itmo.model.SimulationConfig;
import org.junit.jupiter.api.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Стресс-тест полной симуляции.
 * 3 модели (A, B, C) обмениваются случайным количеством агентов (1-1000)
 * на протяжении 50 шагов моделирования.
 */
@Tag("integration-test")
@Timeout(120) // Даем 2 минуты на завершение
class FullOrchestraStressTest {

    private KafkaOrchestrator orchestrator;
    private KafkaOrchestrant modelA, modelB, modelC;
    private AgentSendingEngine engineA, engineB, engineC;
    private AgentCountingReceiver receiverA, receiverB, receiverC;

    @BeforeEach
    void setUp() {
        // Конфигурация для 3 моделей
        List<SimulationConfig> configs = Arrays.asList(
                new SimulationConfig("model-A", "fake.bat", ChronoUnit.SECONDS, LocalDateTime.now(), LocalDateTime.now().plusHours(1), false),
                new SimulationConfig("model-B", "fake.bat", ChronoUnit.SECONDS, LocalDateTime.now(), LocalDateTime.now().plusHours(1), false),
                new SimulationConfig("model-C", "fake.bat", ChronoUnit.SECONDS, LocalDateTime.now(), LocalDateTime.now().plusHours(1), false)
        );

        // Создаем оркестратор (шаг = 1.0)
        orchestrator = new KafkaOrchestrator(configs, 1.0, true, false);

        // Создаем движки (каждый отправляет агенту следующему в кольце)
        engineA = new AgentSendingEngine("model-A", "model-B", 50.0);
        engineB = new AgentSendingEngine("model-B", "model-C", 50.0);
        engineC = new AgentSendingEngine("model-C", "model-A", 50.0);

        // Создаем приемники (считаем, сколько агентов дошло)
        receiverA = new AgentCountingReceiver();
        receiverB = new AgentCountingReceiver();
        receiverC = new AgentCountingReceiver();

        // Создаем оркестрантов
        modelA = new KafkaOrchestrant("model-A", engineA, receiverA, false);
        modelB = new KafkaOrchestrant("model-B", engineB, receiverB, false);
        modelC = new KafkaOrchestrant("model-C", engineC, receiverC, false);

        // Связываем движки с их оркестрантами (чтобы они могли отправлять агентов)
        engineA.setOrchestrant(modelA);
        engineB.setOrchestrant(modelB);
        engineC.setOrchestrant(modelC);
    }

    @AfterEach
    void tearDown() {
        // 1. Сначала останавливаем модели (потребители и heartbeat)
        if (modelA != null) modelA.stop();
        if (modelB != null) modelB.stop();

        // 2. Затем останавливаем оркестратор
        if (orchestrator != null) {
            orchestrator.stopOrchestrator();
        }
    }

    @Test
    void fullSimulation_50StepsWithMassiveAgentTransfer() throws Exception {
        System.out.println("=== STARTING STRESS TEST: 3 Models, 50 Steps, 1-1000 Agents per step ===");

        orchestrator.startOrchestrator();
        Thread.sleep(3000); // Ждем инициализации топиков

        modelA.init();
        modelB.init();
        modelC.init();

        // Ждем, пока все модели выполнят 50 шагов
        long deadline = System.currentTimeMillis() + 90_000; // 90 секунд максимум
        while (System.currentTimeMillis() < deadline && engineA.getRunFastCount() < 50) {
            Thread.sleep(500);
        }

        // Проверяем, что симуляция прошла ровно 50 шагов
        assertEquals(50, engineA.getRunFastCount(), "Model A must execute 50 steps");
        assertEquals(50, engineB.getRunFastCount(), "Model B must execute 50 steps");
        assertEquals(50, engineC.getRunFastCount(), "Model C must execute 50 steps");

        // Даем время последним агентам долететь и обработаться
        Thread.sleep(3000);

        System.out.println("\n--- FINAL AGENT TRANSFER SUMMARY ---");
        System.out.println("Model A: sent=" + engineA.getSentAgents() + ", received=" + receiverA.getReceivedCount());
        System.out.println("Model B: sent=" + engineB.getSentAgents() + ", received=" + receiverB.getReceivedCount());
        System.out.println("Model C: sent=" + engineC.getSentAgents() + ", received=" + receiverC.getReceivedCount());
        System.out.println("------------------------------------\n");

        // Проверяем, что каждая модель и отправила, и получила агентов
        assertTrue(engineA.getSentAgents() > 0 && receiverA.getReceivedCount() > 0, "Model A must send and receive agents");
        assertTrue(engineB.getSentAgents() > 0 && receiverB.getReceivedCount() > 0, "Model B must send and receive agents");
        assertTrue(engineC.getSentAgents() > 0 && receiverC.getReceivedCount() > 0, "Model C must send and receive agents");
    }

    // ==================================================================
    //  Вспомогательные классы
    // ==================================================================

    /**
     * Движок, который в конце каждого шага генерирует от 1 до 1000 агентов
     * и отправляет их целевому оркестранту.
     */
    static class AgentSendingEngine extends TestEngine {
        private final String modelId;
        private final String targetModelId;
        private final Random random = new Random();
        private KafkaOrchestrant orchestrant;
        private final AtomicInteger sentAgents = new AtomicInteger(0);

        public AgentSendingEngine(String modelId, String targetModelId, double stopTime) {
            super(0.0, stopTime, 10); // 10 мс задержки на шаг
            this.modelId = modelId;
            this.targetModelId = targetModelId;
        }

        public void setOrchestrant(KafkaOrchestrant orchestrant) {
            this.orchestrant = orchestrant;
        }

        @Override
        public void executeStepEndEvent(double duration) {
            super.executeStepEndEvent(duration);
            if (orchestrant != null) {
                int count = random.nextInt(1000) + 1; // 1..1000
                sentAgents.addAndGet(count);
                for (int i = 0; i < count; i++) {
                    TransferObject agent = new TransferObject(i, 1, time(), 1, 1, 10.0);
                    orchestrant.sendAgentToOrchestrator(agent, targetModelId);
                }
            }
        }

        public int getSentAgents() {
            return sentAgents.get();
        }
    }

    /**
     * Приемник, который просто считает количество пришедших агентов.
     */
    static class AgentCountingReceiver implements IReceivingAgent {
        private final AtomicInteger receivedCount = new AtomicInteger(0);

        @Override
        public void receiveAgentFromOrchestrator(byte[] data) {
            receivedCount.incrementAndGet();
        }

        public int getReceivedCount() {
            return receivedCount.get();
        }
    }
}