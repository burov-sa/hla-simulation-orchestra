package ru.itmo.infrastructure;

import infrastructure.Protobufmessages;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public final class KafkaOrchestrant {
    private final String id;
    public boolean isDebug = false;
    public IReceivingAgent receivingAgent;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    @Getter
    private volatile boolean firstStepFinished = false;
    @Setter
    private volatile boolean readyForStep = false;

    @Getter
    private IEngine engine;
    @Getter
    private KafkaProducer<String, byte[]> producer;
    public java.util.Queue<Runnable> tasksFromKafka = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private HeartbeatOrchestrantService heartbeat;
    private ConsumerOrchestrantService consumer;
    private RegistrationOrchestrantService registration;

    private static final String MODEL_REGISTRY_TOPIC = "model-registry";
    private static final String ORCHESTRATOR_INBOX = "orchestrator-inbox";
    private static final String SIMULATION_CONTROL_TOPIC = "simulation-control";
    private static final String HEARTBEAT_TOPIC = "model-heartbeat";
    @Getter
    private String inboxTopic;

    public KafkaOrchestrant(String id, IEngine engine, IReceivingAgent reseivingAgent, boolean debugMode) {
        this.id = id;
        this.engine = engine;
        this.receivingAgent = reseivingAgent;
        this.isDebug = debugMode;
        this.inboxTopic = id + "-inbox";
    }

    public String getID() { return this.id; }
    public boolean isRunning() { return this.running.get(); }
    public boolean isWaiting() { return this.readyForStep; }

    public String getOrchestratorInbox() { return this.ORCHESTRATOR_INBOX; }
    public String getSimulationControlTopic() { return this.SIMULATION_CONTROL_TOPIC; }
    public String getModelRegistyTopic() { return this.MODEL_REGISTRY_TOPIC; }
    public String getHeartbeatTopic() { return this.HEARTBEAT_TOPIC; }

    public void setTrueFirstStepFinished() { this.firstStepFinished = true; }

    public void setRunning(boolean value) { this.running.set(value); }

    public void init() {
        Properties prodProps = new Properties();
        //prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getProperty("kafka.bootstrap.servers", "localhost:9092"));
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producer = new KafkaProducer<>(prodProps);

        running.set(true);
        consumer = new ConsumerOrchestrantService(this);
        consumer.start();

        heartbeat = new HeartbeatOrchestrantService(this);
        heartbeat.start();

        registration = new RegistrationOrchestrantService(this);
        registration.subscribe();

        if (engine != null) {
            engine.executeFirstStepEvent();
            this.readyForStep = true;
        }
    }

    public void exit() {
        if (!shuttingDown.compareAndSet(false, true)) return;
        performShutdown();
    }

    public void stop() {
        if (!shuttingDown.compareAndSet(false, true)) return;
        if (registration != null) {
            try { registration.unsubscribe(); } catch (Exception e) {
                log.error("Error sending OFFLINE: " + e.getMessage());
            }
        }
        performShutdown();
    }

    private void performShutdown() {
        if (engine != null) {
            try { engine.pause(); } catch (Exception e) {
                log.error("Engine pause error", e); }
        }

        stopped.set(true);
        running.set(false);

        if (consumer != null) try { consumer.stop(); } catch (Exception e) { log.error("Consumer stop error", e); }
        if (heartbeat != null) try { heartbeat.stop(); } catch (Exception e) { log.error("Heartbeat stop error", e); }

        if (engine != null) {
            try { engine.experimentClose(); } catch (Exception e) { }
            try { engine.stop(); } catch (Exception e) { }
        }

        if (producer != null) {
            try { producer.close(); } catch (Exception e) { }
        }

        if (engine != null) log.info("model time {} : Model is shutdown", engine.time());
        else log.info("Model is shutdown");
    }

    public static void joinThread(Thread thread) {
        if (thread == null) return;
        thread.interrupt();
        try { thread.join(5000); } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Warning: waiting threat join() has been interrupted: {}", thread.getName());
        }
    }

    public void sendAgentToOrchestrator(TransferObject agent, String idDestination) {
        Protobufmessages.AgentMessage agentMsg = Protobufmessages.AgentMessage.newBuilder()
                .setAgentId(agent.getAgentId())
                .setSourceId(agent.getSourceId())
                .setCreationTime(agent.getCreationTime())
                .setCategoryId(agent.getCategoryId())
                .setCategoryFlowId(agent.getCategoryFlowId())
                .setVolume(agent.getVolume())
                .build();

        Protobufmessages.AgentTransfer transfer = Protobufmessages.AgentTransfer.newBuilder()
                .setTargetModelId(idDestination)
                .setAgent(agentMsg)
                .build();

        Protobufmessages.ModelMessage msg = Protobufmessages.ModelMessage.newBuilder()
                .setSenderModelId(id)
                .setAgentTransfer(transfer)
                .build();

        producer.send(new ProducerRecord<>(ORCHESTRATOR_INBOX, msg.toByteArray()));
        if (isDebug) System.out.println("model time " + engine.time() + " ms: agent has been sent to " + idDestination);
    }
    public void sendStepCompleted() {
        Protobufmessages.ModelMessage msg = Protobufmessages.ModelMessage.newBuilder()
                .setSenderModelId(id)
                .setStepCompleted(Protobufmessages.StepCompleted.getDefaultInstance())
                .build();
        producer.send(new ProducerRecord<>(ORCHESTRATOR_INBOX, msg.toByteArray()));
    }

    public void sendStepCompletedAndFinished() {
        Protobufmessages.ModelMessage msg = Protobufmessages.ModelMessage.newBuilder()
                .setSenderModelId(id)
                .setStepCompletedAndFinished(Protobufmessages.StepCompletedAndFinished.getDefaultInstance())
                .build();
        producer.send(new ProducerRecord<>(ORCHESTRATOR_INBOX, msg.toByteArray()));
        producer.flush();
    }
}