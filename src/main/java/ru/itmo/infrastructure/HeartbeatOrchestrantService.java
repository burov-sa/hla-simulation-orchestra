package ru.itmo.infrastructure;

import infrastructure.Protobufmessages;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;

@Log4j2
public final class HeartbeatOrchestrantService {
    private static final long INTERVAL_MS = 10_000;
    private Thread heartbeatThread;
    private final KafkaOrchestrant owner;

    public HeartbeatOrchestrantService(KafkaOrchestrant owner) { this.owner = owner; }

    public void start() {
        heartbeatThread = new Thread(() -> {
            while (owner.isRunning()) {
                try {
                    Thread.sleep(INTERVAL_MS);
                    if (owner.getProducer() != null) {
                        var msg = Protobufmessages.ModelMessage.newBuilder()
                                .setSenderModelId(owner.getID())
                                .setHeartbeat(Protobufmessages.Heartbeat.getDefaultInstance())
                                .build();
                        owner.getProducer().send(new ProducerRecord<>(owner.getHeartbeatTopic(), msg.toByteArray()));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Heartbeat error", e);
                }
            }
        });
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    public void stop() { owner.joinThread(heartbeatThread); }
}