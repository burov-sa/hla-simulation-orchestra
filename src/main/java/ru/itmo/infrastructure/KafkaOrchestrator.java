package ru.itmo.infrastructure;

import infrastructure.Protobufmessages;
import com.google.protobuf.InvalidProtocolBufferException;
import ru.itmo.model.SimulationConfig;
import ru.itmo.model.SimulationConfigLoader;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.nio.charset.StandardCharsets;
import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.format.DateTimeFormatter;

/**
 * Оркестратор для AnyLogic.
 * Все сообщения передаются в формате Protobuf.
 */
public class KafkaOrchestrator {

    // --- Параметры оркестратора ---
    private final List<SimulationConfig> simulationConfigs;
    private final Set<String> expectedModelIds;

    // --- Состояние процесса ---
    private final AtomicBoolean stopSimulationSent = new AtomicBoolean(false);
    private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);
    private boolean firstStepSent = false;
    private boolean isDebug = false;
    private final AtomicReference<Double> counterModelTime = new AtomicReference<>(0.0);
    private volatile double stepDuration = 1.0;
    private final boolean cleanOnStart;
    private final ConcurrentHashMap<String, Process> runningModelProcesses = new ConcurrentHashMap<>();

    // --- Kafka настройки ---
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String ORCHESTRATOR_INBOX = "orchestrator-inbox";
    private static final String MODEL_REGISTRY_TOPIC = "model-registry";
    private static final String MODEL_HEARTBEAT_TOPIC = "model-heartbeat";
    private static final String SIMULATION_CONTROL_TOPIC = "simulation-control";
    private static final String DEAD_LETTER_TOPIC = "dead-letter-queue";

    // --- Heartbeat настройки ---
    private static final long HEARTBEAT_TIMEOUT_MS = 60_000;
    private static final long HEARTBEAT_CHECK_INTERVAL_MS = 30_000;

    // --- Данные о моделях ---
    private static class ModelInfo {
        String inboxTopic;
        volatile Instant lastHeartbeat;

        ModelInfo(String inboxTopic, Instant lastHeartbeat) {
            this.inboxTopic = inboxTopic;
            this.lastHeartbeat = lastHeartbeat;
        }
    }

    private static final DateTimeFormatter MODEL_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ss");

    private final Map<String, ModelInfo> activeModels = new ConcurrentHashMap<>();
    private final Set<String> stepReadyModels = ConcurrentHashMap.newKeySet();

    // --- Инфраструктура ---
    private KafkaConsumer<String, byte[]> consumer;
    private KafkaProducer<String, byte[]> producer;
    private final ScheduledExecutorService heartbeatChecker = Executors.newSingleThreadScheduledExecutor();

    private volatile boolean running = true;
    private Thread orchestratorThread;

    public KafkaOrchestrator(List<SimulationConfig> simulationConfigs,
                             double stepDuration,
                             boolean cleanOnStart,
                             boolean debugMode) {
        this.simulationConfigs = simulationConfigs;
        this.stepDuration = stepDuration;
        this.cleanOnStart = cleanOnStart;
        this.expectedModelIds = Collections.unmodifiableSet(new HashSet<>(simulationConfigs.stream()
                .map(SimulationConfig::getId)
                .collect(Collectors.toSet())));
        this.isDebug = debugMode;
    }

    public static KafkaOrchestrator createFromConfig(String configPath,
                                                     double stepDuration,
                                                     boolean cleanOnStart,
                                                     boolean debugMode) {
        try {
            List<SimulationConfig> params = SimulationConfigLoader.loadFromJson(configPath);
            logConfiguration(params);
            return new KafkaOrchestrator(params, stepDuration, cleanOnStart, debugMode);
        } catch (IOException e) {
            System.out.println("ERROR: error initialization from configuration file: " + configPath);
            return null;
        }
    }

    private static boolean isKafkaReady() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000);

        try (AdminClient admin = AdminClient.create(props)) {
            admin.listTopics().names().get(3, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void waitForKafkaReady() {
        int maxRetries = 5;
        long backoffMs = 2000;
        for (int i = 1; i <= maxRetries; i++) {
            if (isKafkaReady()) {
                System.out.println("Kafka is ready to work on " + BOOTSTRAP_SERVERS);
                return;
            }
            System.out.println("Waiting Kafka... attempt " + i + "/" + maxRetries);
            try {
                Thread.sleep(backoffMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Waiting Kafka is interrupted: ", e);
            }
            backoffMs = Math.min(backoffMs * 2, 10_000);
        }
        throw new IllegalStateException("Kafka is not available after " + maxRetries + " attempts.");
    }

    private static void logConfiguration(List<SimulationConfig> params) {
        System.out.println("SUCCESSFULL: Models configuration is loaded: ");
        params.forEach(System.out::println);
    }

    // --- Getters & Setters ---
    public void resetCounterModelTime() {
        counterModelTime.set(0.0);
    }

    public double getCounterModelTime() {
        return counterModelTime.get();
    }

    public void setStepDuration(double dt) {
        this.stepDuration = dt;
    }

    public void startOrchestrator() {
        if (orchestratorThread != null && orchestratorThread.isAlive()) {
            System.out.println("Warning: Orchestrator is already loaded");
            return;
        }
        this.running = true;
        this.firstStepSent = false;
        this.counterModelTime.set(0.0);
        this.activeModels.clear();
        this.stepReadyModels.clear();
        this.stopSimulationSent.set(false);
        this.shutdownInitiated.set(false);

        orchestratorThread = new Thread(() -> {
            try {
                start();
            } catch (Exception e) {
                System.err.print("Orchestrator error: " + e.getMessage());
                e.printStackTrace();
            }
        });
        orchestratorThread.setDaemon(true);
        orchestratorThread.start();

        System.out.println("Orchestrator is loaded in a separate thread");
    }

    public void finishModeling() {
        System.out.println("SIMULATION FINISHED. NO ACTIVE MODELS");
        stopOrchestrator();
    }

    public void stopOrchestrator() {
        stopOrchestre();
        try {
            Thread.sleep(1000); // Даем время на доставку STOP_SIMULATION
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        running = false;
        if (consumer != null) consumer.wakeup();

        if (orchestratorThread != null && Thread.currentThread() != orchestratorThread) {
            try {
                orchestratorThread.join(10000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (orchestratorThread.isAlive()) orchestratorThread.interrupt();
        }
        shutdown();
    }

    public void stopOrchestre() {
        if (stopSimulationSent.compareAndSet(false, true)) {
            for (String modelId : expectedModelIds) {
                sendStopSimulationToModel(modelId);
            }
            System.out.println("Command STOP_SIMULATION has been sent to all Orchestrants");
        } else {
            System.out.println("Warning: Command STOP_SIMULATION has already sent");
        }
    }

    public boolean runModels() {
        boolean atLeastOneLaunched = false;

        for (SimulationConfig simulationConfig : simulationConfigs) {
            String modelId = simulationConfig.getId();
            String batFile = simulationConfig.getFilePath();
            String isDebug = String.valueOf(simulationConfig.isDebug());
            String timeUnit = String.valueOf(simulationConfig.getTimeUnit());

            String startSimulation = simulationConfig.getStartSimulation().format(MODEL_TIME_FORMATTER);
            String stopSimulation = simulationConfig.getStopSimulation().format(MODEL_TIME_FORMATTER);

            String host = "none", port = "none", dbName = "none", userName = "none", password = "none";

            File file = new File(batFile);
            if (!file.exists()) {
                System.out.println("File not found: " + batFile);
                continue;
            }

            try {
                String[] argsForAll = {
                        isDebug, modelId, timeUnit, startSimulation, stopSimulation,
                        host, port, dbName, userName, password
                };

                String[] command = new String[argsForAll.length + 3];
                command[0] = "cmd.exe";
                command[1] = "/c";
                command[2] = batFile;
                System.arraycopy(argsForAll, 0, command, 3, argsForAll.length);

                Process process = Runtime.getRuntime().exec(command);
                runningModelProcesses.put(modelId, process);
                startProcessStreamConsumers(process, modelId);
                atLeastOneLaunched = true;
                System.out.println("Model " + modelId + " has been launched (" + batFile + ")");
            } catch (IOException e) {
                System.err.print("Model launch error: " + modelId + ": " + e.getMessage());
                return false;
            }
        }

        if (!atLeastOneLaunched) {
            System.err.println("No models were launched. Check configuration and file paths.");
            return false;
        }
        return true;
    }

    private void startProcessStreamConsumers(Process process, String modelId) {
        Thread stdoutThread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (isDebug) System.out.println("[" + modelId + " stdout] " + line);
                }
            } catch (IOException ignored) {}
        }, "stdout-consumer-" + modelId);
        stdoutThread.setDaemon(true);
        stdoutThread.start();

        Thread stderrThread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.err.print("[" + modelId + " stderr] " + line);
                }
            } catch (IOException ignored) {}
        }, "stderr-consumer-" + modelId);
        stderrThread.setDaemon(true);
        stderrThread.start();
    }

    private void terminateAllModelProcesses() {
        for (Map.Entry<String, Process> entry : runningModelProcesses.entrySet()) {
            String modelId = entry.getKey();
            Process process = entry.getValue();
            if (process.isAlive()) {
                System.out.println("Completing the model thread " + modelId);
                process.destroy();
                try {
                    if (!process.waitFor(5, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                        System.err.print("Thread " + modelId + " is completed forcibly");
                    }
                } catch (InterruptedException e) {
                    process.destroyForcibly();
                    Thread.currentThread().interrupt();
                }
            }
        }
        runningModelProcesses.clear();
    }

    private void start() {
        try {
            waitForKafkaReady();
            initializeKafka();
            if (cleanOnStart) cleanAllTopics();
            createAllTopics();
            subscribeToTopics();
            startHeartbeatChecker();
            processMessages();
        } finally {
            shutdown();
        }
    }

    private void initializeKafka() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        consumerProps.put("group.id", "simulation-orchestrator");
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerProps.put("auto.offset.reset", "earliest");
        consumer = new KafkaConsumer<>(consumerProps);

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", ByteArraySerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);
    }

    private void createAllTopics() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            List<NewTopic> topics = Arrays.asList(
                    new NewTopic(ORCHESTRATOR_INBOX, 3, (short) 1),
                    new NewTopic(MODEL_REGISTRY_TOPIC, 1, (short) 1),
                    new NewTopic(MODEL_HEARTBEAT_TOPIC, 3, (short) 1),
                    new NewTopic(SIMULATION_CONTROL_TOPIC, 3, (short) 1),
                    new NewTopic(DEAD_LETTER_TOPIC, 1, (short) 1)
            );

            for (NewTopic topic : topics) {
                try {
                    admin.createTopics(Collections.singleton(topic)).all().get();
                    System.out.println("Topic created: " + topic.name());
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof TopicExistsException) {
                        System.out.println("Warning: Topic already exists: " + topic.name());
                    } else {
                        System.err.print("Topic creation error: " + topic.name() + ": " + e.getMessage());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } catch (Exception e) {
            System.err.print("AdminClient initialization error: " + e.getMessage());
        }
    }

    private void deleteTopics(Set<String> topicNames) {
        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS))) {
            DeleteTopicsResult result = admin.deleteTopics(topicNames);
            result.all().get(10, TimeUnit.SECONDS);
            System.out.println("Topics deleted: " + topicNames);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                System.out.println("Attempt to delete non-existent topics: " + topicNames);
            } else {
                System.err.print("Error removing topics: " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.print("Error removing topics: " + e.getMessage());
        }
    }

    private Set<String> getAllTopicsWithPrefix(String prefix) {
        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS))) {
            ListTopicsResult topicsResult = admin.listTopics();
            Set<String> allTopics = topicsResult.names().get(5, TimeUnit.SECONDS);
            return allTopics.stream().filter(name -> name.startsWith(prefix)).collect(Collectors.toSet());
        } catch (Exception e) {
            System.err.print("Error getting the list of topics: " + e.getMessage());
            return Collections.emptySet();
        }
    }

    private void cleanAllTopics() {
        System.out.println("Clearing all topics...");

        Set<String> systemTopics = new HashSet<>(Arrays.asList(
                ORCHESTRATOR_INBOX, MODEL_REGISTRY_TOPIC, MODEL_HEARTBEAT_TOPIC,
                SIMULATION_CONTROL_TOPIC, DEAD_LETTER_TOPIC
        ));

        Set<String> modelTopics = getAllTopicsWithPrefix("model-");
        for (String modelId : expectedModelIds) {
            modelTopics.add(modelId + "-inbox");
        }

        Set<String> allToDelete = new HashSet<>();
        allToDelete.addAll(systemTopics);
        allToDelete.addAll(modelTopics);

        if (!allToDelete.isEmpty()) {
            deleteTopics(allToDelete);
            try { Thread.sleep(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        } else {
            System.out.println("There are no topics to delete");
        }
    }

    private void subscribeToTopics() {
        consumer.subscribe(Arrays.asList(ORCHESTRATOR_INBOX, MODEL_REGISTRY_TOPIC, MODEL_HEARTBEAT_TOPIC));
        System.out.println("Orchestrator subscribed to the topics");
    }

    private void startHeartbeatChecker() {
        heartbeatChecker.scheduleAtFixedRate(this::checkHeartbeats,
                HEARTBEAT_CHECK_INTERVAL_MS, HEARTBEAT_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void processMessages() {
        while (running) {
            try {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, byte[]> record : records) {
                    String topic = record.topic();
                    if (MODEL_REGISTRY_TOPIC.equals(topic)) handleRegistryMessage(record.value());
                    else if (MODEL_HEARTBEAT_TOPIC.equals(topic)) handleHeartbeatMessage(record.value());
                    else if (ORCHESTRATOR_INBOX.equals(topic)) handleOrchestratorMessage(record.value());
                }
            } catch (WakeupException e) {
                break;
            } catch (Exception e) {
                System.err.println("Error in the main loop: " + e.getMessage());
            }
        }
    }

    private void handleRegistryMessage(byte[] raw) {
        try {
            Protobufmessages.ModelMessage msg = Protobufmessages.ModelMessage.parseFrom(raw);
            if (msg.getKindCase() == Protobufmessages.ModelMessage.KindCase.REGISTRATION) {
                String modelId = msg.getSenderModelId();
                var reg = msg.getRegistration();

                if (reg.getIsOnline()) {
                    String inboxTopic = reg.getInboxTopic();
                    createTopicIfNotExists(inboxTopic);

                    activeModels.compute(modelId, (id, existing) -> {
                        if (existing == null) {
                            System.out.println("Model is registered: " + modelId + " (topic " + inboxTopic + ")");
                            return new ModelInfo(inboxTopic, Instant.now());
                        } else {
                            existing.inboxTopic = inboxTopic;
                            existing.lastHeartbeat = Instant.now();
                            return existing;
                        }
                    });

                    if (!firstStepSent && activeModels.keySet().containsAll(expectedModelIds)) {
                        firstStepSent = true;
                        broadcastNextStep();
                    }
                } else {
                    activeModels.remove(modelId);
                    stepReadyModels.remove(modelId);
                    System.out.println("MODEL " + modelId + " OFF (finish simulation). Count activeModels = " + activeModels.size());
                    checkAndCompleteBarrier();
                }
            }
        } catch (Exception e) {
            System.err.print("Error registration: " + e.getMessage());
        }
    }

    private void createTopicIfNotExists(String topicName) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            NewTopic topic = new NewTopic(topicName, 1, (short) 1);
            admin.createTopics(Collections.singleton(topic)).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                System.err.print("Topic created error " + topicName + ": " + e.getMessage());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {}
    }

    private void handleHeartbeatMessage(byte[] raw) {
        try {
            Protobufmessages.ModelMessage msg = Protobufmessages.ModelMessage.parseFrom(raw);
            if (msg.getKindCase() == Protobufmessages.ModelMessage.KindCase.HEARTBEAT) {
                ModelInfo info = activeModels.get(msg.getSenderModelId());
                if (info != null) {
                    info.lastHeartbeat = Instant.now();
                    System.out.println(LocalTime.now() + " ok " + msg.getSenderModelId());
                }
            }
        } catch (Exception e) {
            System.err.print("Heartbeat error: " + e.getMessage());
        }
    }

    private void handleOrchestratorMessage(byte[] raw) {
        try {
            Protobufmessages.ModelMessage msg = Protobufmessages.ModelMessage.parseFrom(raw);
            String sender = msg.getSenderModelId();

            switch (msg.getKindCase()) {
                case AGENT_TRANSFER -> {
                    var transfer = msg.getAgentTransfer();
                    String target = transfer.getTargetModelId();
                    ModelInfo targetInfo = activeModels.get(target);

                    if (targetInfo == null) {
                        sendToDeadLetterQueue(raw, "Attempt to sent agents to a non-working model: " + target);
                    } else {
                        Protobufmessages.OrchestratorMessage outMsg = Protobufmessages.OrchestratorMessage.newBuilder()
                                .setAgentTransfer(transfer)
                                .build();

                        producer.send(new ProducerRecord<>(targetInfo.inboxTopic, outMsg.toByteArray()),
                                (metadata, exception) -> {
                                    if (exception != null) {
                                        System.err.print("Sending error to agent: " + exception.getMessage());
                                        sendToDeadLetterQueue(raw, "Send failed");
                                    }
                                });
                    }
                }
                case STEP_COMPLETED -> handleStepCompleted(sender, false);
                case STEP_COMPLETED_AND_FINISHED -> handleStepCompleted(sender, true);
                case KIND_NOT_SET -> {}
            }
            if (this.isDebug) {
                System.out.println(msg.getKindCase() + " from " + sender);
            }
        } catch (InvalidProtocolBufferException e) {
            System.err.print("Protobuf deserialization error: " + e.getMessage());
            sendToDeadLetterQueue(raw, "Invalid Protobuf format: " + e.getMessage());
        }
    }

    private void handleStepCompleted(String modelId, boolean isFinished) {
        if (!activeModels.containsKey(modelId)) {
            return;
        }
        if (!isFinished) {
            synchronized (stepReadyModels) {
                stepReadyModels.add(modelId);
                if (this.isDebug) {
                    System.out.println("Model " + modelId + " completed step. Done: " + stepReadyModels.size() + "/" + activeModels.size());
                }
            }
        } else {
            synchronized (activeModels) {
                activeModels.remove(modelId);
            }
            synchronized (stepReadyModels) {
                stepReadyModels.remove(modelId);
            }
            System.out.println("Model " + modelId + " completed step and finished simulation");
        }
        checkAndCompleteBarrier();
    }

    private void checkAndCompleteBarrier() {
        synchronized (activeModels) {
            if (activeModels.isEmpty()) {
                finishModeling();
                return;
            }
        }
        synchronized (stepReadyModels) {
            if (stepReadyModels.size() >= activeModels.size()) {
                stepReadyModels.clear();
                counterModelTime.updateAndGet(v -> v + this.stepDuration);
                broadcastNextStep();
            }
        }
    }

    private void broadcastNextStep() {
        for (String modelId : activeModels.keySet()) {
            sendNextStepToModel(modelId);
        }
    }

    private void sendNextStepToModel(String modelId) {
        try {
            Protobufmessages.OrchestratorMessage msg = Protobufmessages.OrchestratorMessage.newBuilder()
                    .setNextStep(Protobufmessages.NextStepCommand.newBuilder()
                            .setNextSimulationTime(counterModelTime.get() + this.stepDuration))
                    .build();

            producer.send(new ProducerRecord<>(SIMULATION_CONTROL_TOPIC, modelId, msg.toByteArray()),
                    (metadata, exception) -> {
                        if (exception != null) {
                            System.err.print("Error to sending command NEXT_STEP to model " + modelId + ": " + exception.getMessage());
                        } else {
                            if (this.isDebug) {
                                System.out.println("NEXT_STEP has been sent to model " + modelId);
                            }
                        }
                    });
        } catch (Exception e) {
            System.err.print("Command created error: " + e.getMessage());
        }
    }

    private void sendStopSimulationToModel(String modelId) {
        try {
            Protobufmessages.OrchestratorMessage msg = Protobufmessages.OrchestratorMessage.newBuilder()
                    .setStopSimulation(Protobufmessages.StopSimulationCommand.getDefaultInstance())
                    .build();

            producer.send(new ProducerRecord<>(SIMULATION_CONTROL_TOPIC, modelId, msg.toByteArray()),
                    (metadata, exception) -> {
                        if (exception != null) {
                            System.err.print("Error to sending command STOP_SIMULATION to model " + modelId + ": " + exception.getMessage());
                        } else {
                            System.out.println("STOP_SIMULATION has been sent to model " + modelId);
                        }
                    });
        } catch (Exception e) {
            System.err.print("Command created error: " + e.getMessage());
        }
    }

    private void checkHeartbeats() {
        try {
            Instant now = Instant.now();
            List<String> dead = new ArrayList<>();
            for (Map.Entry<String, ModelInfo> e : activeModels.entrySet()) {
                if (e.getValue().lastHeartbeat.plusMillis(HEARTBEAT_TIMEOUT_MS).isBefore(now)) {
                    dead.add(e.getKey());
                }
            }
            for (String id : dead) {
                synchronized (activeModels) {
                    activeModels.remove(id);
                }
                synchronized (stepReadyModels) {
                    stepReadyModels.remove(id);
                }
                System.out.println("MODEL " + id + " OFF (heartbeat timeout)");
            }
            if (!dead.isEmpty()) checkAndCompleteBarrier();
        } catch (Exception e) {
            System.err.print("Heartbeat thread error " + e.getMessage());
        }
    }

    private void sendToDeadLetterQueue(byte[] originalMessage, String reason) {
        try {
            // Формируем простую текстовую строку для DLQ
            String dlqText = reason + " | Original (Base64): " + Base64.getEncoder().encodeToString(originalMessage);
            producer.send(new ProducerRecord<>(DEAD_LETTER_TOPIC, dlqText.getBytes(StandardCharsets.UTF_8)));
            System.out.println("Message has been sent to DLQ: " + reason);
        } catch (Exception e) {
            System.err.print("Error sending to DLQ: " + e.getMessage());
        }
    }

    private void shutdown() {
        if (!shutdownInitiated.compareAndSet(false, true)) return;
        running = false;
        Thread.interrupted();

        if (producer != null) try { producer.flush(); } catch (Exception ignored) {}
        if (consumer != null) try { consumer.close(); } catch (Exception ignored) {}
        if (producer != null) try { producer.close(); } catch (Exception ignored) {}

        heartbeatChecker.shutdown();
        try {
            if (!heartbeatChecker.awaitTermination(5, TimeUnit.SECONDS)) {
                heartbeatChecker.shutdownNow();
            }
        } catch (InterruptedException e) {
            heartbeatChecker.shutdownNow();
            Thread.currentThread().interrupt();
        }

        terminateAllModelProcesses();
        System.out.println("Orchestrator's resources are free");
    }

    @Override
    public String toString() {
        StringBuilder modelIds = new StringBuilder();
        modelIds.append("models={");
        expectedModelIds.forEach(id -> {
            modelIds.append(id);
            modelIds.append(",");
        });
        if (modelIds.length() > 8) modelIds.deleteCharAt(modelIds.length() - 1);
        modelIds.append("}");

        return "Orchestrator<" + modelIds.toString() + ",\n" +
                "timeUnit=" + this.simulationConfigs.get(0).getTimeUnit() + ",\n" +
                "startDate=" + this.simulationConfigs.get(0).getStartSimulation() + ",\n" +
                "stopDate=" + this.simulationConfigs.get(0).getStopSimulation() + ">";
    }
}