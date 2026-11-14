package ru.itmo.infrastructure;

import lombok.extern.log4j.Log4j2;
import ru.itmo.model.SimulationConfig;
import org.junit.jupiter.api.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration-test")
@Timeout(180)
@Log4j2
class FullOrchestraMeshStressTest extends AbstractKafkaIntegrationTest { // <-- ДОБАВЛЕНО

    private KafkaOrchestrator orchestrator;
    private KafkaOrchestrant modelA, modelB, modelC;
    private AgentSendingMeshEngine engineA, engineB, engineC;
    private AgentCountingReceiver receiverA, receiverB, receiverC;

    @BeforeEach
    void setUp() {
        List<SimulationConfig> configs = Arrays.asList(
                new SimulationConfig("model-A", "fake.bat", ChronoUnit.SECONDS, LocalDateTime.now(), LocalDateTime.now().plusHours(1), false),
                new SimulationConfig("model-B", "fake.bat", ChronoUnit.SECONDS, LocalDateTime.now(), LocalDateTime.now().plusHours(1), false),
                new SimulationConfig("model-C", "fake.bat", ChronoUnit.SECONDS, LocalDateTime.now(), LocalDateTime.now().plusHours(1), false)
        );

        orchestrator = new KafkaOrchestrator(configs, 1.0, true, false);

        engineA = new AgentSendingMeshEngine("model-A", List.of("model-B", "model-C"), 50.0);
        engineB = new AgentSendingMeshEngine("model-B", List.of("model-A", "model-C"), 50.0);
        engineC = new AgentSendingMeshEngine("model-C", List.of("model-A", "model-B"), 50.0);

        receiverA = new AgentCountingReceiver();
        receiverB = new AgentCountingReceiver();
        receiverC = new AgentCountingReceiver();

        modelA = new KafkaOrchestrant("model-A", engineA, receiverA, false);
        modelB = new KafkaOrchestrant("model-B", engineB, receiverB, false);
        modelC = new KafkaOrchestrant("model-C", engineC, receiverC, false);

        engineA.setOrchestrant(modelA);
        engineB.setOrchestrant(modelB);
        engineC.setOrchestrant(modelC);
    }

    @AfterEach
    void tearDown() {
        if (modelA != null) modelA.stop();
        if (modelB != null) modelB.stop();
        if (modelC != null) modelC.stop();

        if (orchestrator != null) {
            orchestrator.stopOrchestrator();
        }
    }

    @Test
    void fullMeshSimulation_50StepsWithMassiveAgentTransfer() throws Exception {
        log.info("=== STARTING FULL MESH STRESS TEST: 3 Models, 50 Steps, 1-1000 Agents to EACH target per step ===");

        orchestrator.startOrchestrator();
        Thread.sleep(3000);

        modelA.init();
        modelB.init();
        modelC.init();

        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline && engineA.getRunFastCount() < 50) {
            Thread.sleep(500);
        }

        assertEquals(50, engineA.getRunFastCount(), "Model A must execute 50 steps");
        assertEquals(50, engineB.getRunFastCount(), "Model B must execute 50 steps");
        assertEquals(50, engineC.getRunFastCount(), "Model C must execute 50 steps");

        Thread.sleep(3000);

        log.info("\n--- FINAL MESH AGENT TRANSFER SUMMARY ---");
        log.info("Model A: sent={}, received={}", engineA.getSentAgents(), receiverA.getReceivedCount());
        log.info("Model B: sent={}, received={}", engineB.getSentAgents(), receiverB.getReceivedCount());
        log.info("Model C: sent={}, received={}", engineC.getSentAgents(), receiverC.getReceivedCount());
        log.info("-----------------------------------------\n");

        assertTrue(engineA.getSentAgents() > 0 && receiverA.getReceivedCount() > 0, "Model A must send and receive agents");
        assertTrue(engineB.getSentAgents() > 0 && receiverB.getReceivedCount() > 0, "Model B must send and receive agents");
        assertTrue(engineC.getSentAgents() > 0 && receiverC.getReceivedCount() > 0, "Model C must send and receive agents");
    }

    static final class AgentSendingMeshEngine extends TestEngine {
        private final List<String> targetModelIds;
        private final Random random = new Random();
        private KafkaOrchestrant orchestrant;
        private final AtomicInteger sentAgents = new AtomicInteger(0);

        public AgentSendingMeshEngine(String modelId, List<String> targetModelIds, double stopTime) {
            super(0.0, stopTime, 10);
            this.targetModelIds = targetModelIds;
        }

        public void setOrchestrant(KafkaOrchestrant orchestrant) {
            this.orchestrant = orchestrant;
        }

        @Override
        public void executeStepEndEvent(double duration) {
            super.executeStepEndEvent(duration);
            if (orchestrant != null) {
                for (String targetId : targetModelIds) {
                    int count = random.nextInt(1000) + 1;
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