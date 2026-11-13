package ru.itmo.infrastructure;

import ru.itmo.model.SimulationConfig;
import org.junit.jupiter.api.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Стресс-тест полной симуляции с полносвязной топологией (Full Mesh).
 * 3 модели (A, B, C) обмениваются случайным количеством агентов (1-1000)
 * друг с другом (каждая с каждой) на протяжении 50 шагов.
 */
@Tag("integration-test")
@Timeout(180) // Увеличен таймаут, так как сообщений будет в 2 раза больше, чем в кольцевом тесте
class FullOrchestraMeshStressTest {

    private KafkaOrchestrator orchestrator;
    private KafkaOrchestrant modelA, modelB, modelC;
    private AgentSendingMeshEngine engineA, engineB, engineC;
    private AgentCountingReceiver receiverA, receiverB, receiverC;

    @BeforeEach
    void setUp() {
        // Конфигурация для 3 моделей
        List<SimulationConfig> configs = Arrays.asList(
                new SimulationConfig("model-A", "fake.bat", ChronoUnit.SECONDS, LocalDateTime.now(), LocalDateTime.now().plusHours(1), false),
                new SimulationConfig("model-B", "fake.bat", ChronoUnit.SECONDS, LocalDateTime.now(), LocalDateTime.now().plusHours(1), false),
                new SimulationConfig("model-C", "fake.bat", ChronoUnit.SECONDS, LocalDateTime.now(), LocalDateTime.now().plusHours(1), false)
        );

        // Оркестратор (шаг = 1.0)
        orchestrator = new KafkaOrchestrator(configs, 1.0, true, false);

        // Создаем движки (каждый отправляет агентов ОБОИМ другим моделям)
        engineA = new AgentSendingMeshEngine("model-A", List.of("model-B", "model-C"), 50.0);
        engineB = new AgentSendingMeshEngine("model-B", List.of("model-A", "model-C"), 50.0);
        engineC = new AgentSendingMeshEngine("model-C", List.of("model-A", "model-B"), 50.0);

        // Создаем приемники
        receiverA = new AgentCountingReceiver();
        receiverB = new AgentCountingReceiver();
        receiverC = new AgentCountingReceiver();

        // Создаем оркестрантов
        modelA = new KafkaOrchestrant("model-A", engineA, receiverA, false);
        modelB = new KafkaOrchestrant("model-B", engineB, receiverB, false);
        modelC = new KafkaOrchestrant("model-C", engineC, receiverC, false);

        // Связываем движки с их оркестрантами
        engineA.setOrchestrant(modelA);
        engineB.setOrchestrant(modelB);
        engineC.setOrchestrant(modelC);
    }

    @AfterEach
    void tearDown() {
        // 1. Сначала останавливаем модели (потребители и heartbeat)
        if (modelA != null) modelA.stop();
        if (modelB != null) modelB.stop();
        if (modelC != null) modelC.stop();

        // 2. Затем останавливаем оркестратор
        if (orchestrator != null) {
            orchestrator.stopOrchestrator();
        }
    }

    @Test
    void fullMeshSimulation_50StepsWithMassiveAgentTransfer() throws Exception {
        System.out.println("=== STARTING FULL MESH STRESS TEST: 3 Models, 50 Steps, 1-1000 Agents to EACH target per step ===");

        orchestrator.startOrchestrator();
        Thread.sleep(3000); // Ждем инициализации топиков

        modelA.init();
        modelB.init();
        modelC.init();

        // Ждем, пока все модели выполнят 50 шагов
        long deadline = System.currentTimeMillis() + 120_000; // 120 секунд максимум
        while (System.currentTimeMillis() < deadline && engineA.getRunFastCount() < 50) {
            Thread.sleep(500);
        }

        // Проверяем, что симуляция прошла ровно 50 шагов
        assertEquals(50, engineA.getRunFastCount(), "Model A must execute 50 steps");
        assertEquals(50, engineB.getRunFastCount(), "Model B must execute 50 steps");
        assertEquals(50, engineC.getRunFastCount(), "Model C must execute 50 steps");

        // Даем время последним агентам долететь и обработаться
        Thread.sleep(3000);

        System.out.println("\n--- FINAL MESH AGENT TRANSFER SUMMARY ---");
        System.out.println("Model A: sent=" + engineA.getSentAgents() + ", received=" + receiverA.getReceivedCount());
        System.out.println("Model B: sent=" + engineB.getSentAgents() + ", received=" + receiverB.getReceivedCount());
        System.out.println("Model C: sent=" + engineC.getSentAgents() + ", received=" + receiverC.getReceivedCount());
        System.out.println("-----------------------------------------\n");

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
     * для КАЖДОГО целевого оркестранта из списка.
     */
    static class AgentSendingMeshEngine extends TestEngine {
        private final List<String> targetModelIds;
        private final Random random = new Random();
        private KafkaOrchestrant orchestrant;
        private final AtomicInteger sentAgents = new AtomicInteger(0);

        public AgentSendingMeshEngine(String modelId, List<String> targetModelIds, double stopTime) {
            super(0.0, stopTime, 10); // 10 мс задержки на шаг
            this.targetModelIds = targetModelIds;
        }

        public void setOrchestrant(KafkaOrchestrant orchestrant) {
            this.orchestrant = orchestrant;
        }

        @Override
        public void executeStepEndEvent(double duration) {
            super.executeStepEndEvent(duration);
            if (orchestrant != null) {
                // Отправляем агентов каждому из целевых партнеров
                for (String targetId : targetModelIds) {
                    int count = random.nextInt(1000) + 1; // 1..1000
                    sentAgents.addAndGet(count);
                    for (int i = 0; i < count; i++) {
                        TransferObject agent = new TransferObject(i, 1, time(), 1, 1, 10.0);
                        orchestrant.sendAgentToOrchestrator(agent, targetId);
                    }
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