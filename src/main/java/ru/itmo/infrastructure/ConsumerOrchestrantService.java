package ru.itmo.infrastructure;

import infrastructure.Protobufmessages;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerOrchestrantService {
    private final KafkaOrchestrant owner;
    private Thread commandThread;
    private Thread consumerThread;
    private KafkaConsumer<String, byte[]> cmdConsumer;
    private KafkaConsumer<String, byte[]> consumer;
    private final IEngine engine;

    public ConsumerOrchestrantService(KafkaOrchestrant owner) {
        this.owner = owner;
        this.engine = owner.getEngine();
    }

    public void start() {
        // ==========================================
        // 1. COMMAND THREAD (Управление шагами)
        // ==========================================
        Properties cmdProps = new Properties();
        cmdProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        cmdProps.put(ConsumerConfig.GROUP_ID_CONFIG, owner.getID() + "-commands");
        cmdProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cmdProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        cmdProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cmdConsumer = new KafkaConsumer<>(cmdProps);
        cmdConsumer.subscribe(Collections.singletonList(owner.getSimulationControlTopic()));

        commandThread = new Thread(() -> {
            while (owner.isRunning()) {
                try {
                    ConsumerRecords<String, byte[]> records = cmdConsumer.poll(Duration.ofMillis(100));
                    for (var record : records) {
                        // Отбрасываем команды, адресованные другим моделям
                        if (record.key() != null && !owner.getID().equals(record.key())) continue;

                        try {
                            var msg = Protobufmessages.OrchestratorMessage.parseFrom(record.value());

                            if (msg.hasNextStep() && owner.isWaiting()) {
                                double time = engine.time();
                                double nextTime = msg.getNextStep().getNextSimulationTime();

                                if (nextTime > time) {
                                    // 1. Вычитываем агентов, накопившихся до начала шага
                                    Runnable task;
                                    while ((task = owner.tasksFromKafka.poll()) != null) {
                                        task.run();
                                    }

                                    if (nextTime > engine.getStopTime()) nextTime = engine.getStopTime();
                                    double duration = nextTime - time;

                                    if (!owner.isFirstStepFinished() || owner.isDebug) {
                                        System.out.println("model time " + engine.time() + " : received NEXT_STEP, duration = " + duration);
                                        owner.setTrueFirstStepFinished();
                                    }

                                    // 2. Планируем события шага (для AnyLogic это DynamicEvent)
                                    engine.executeStepStartEvent();
                                    engine.executeStepEndEvent(duration);

                                    // 3. Запускаем шаг (метод блокирующий)
                                    owner.setReadyForStep(false);
                                    boolean started = engine.runFast();

                                    // 4. Шаг завершен, снова готовы к приему команд
                                    owner.setReadyForStep(true);

                                    if (started) {
                                        if (owner.isDebug) {
                                            System.out.println("model time " + engine.time() + " : step completed");
                                        }

                                        // 5. Отправляем оркестратору отчет о завершении шага
                                        if (engine.time() >= engine.getStopTime()) {
                                            owner.sendStepCompletedAndFinished();
                                        } else {
                                            owner.sendStepCompleted();
                                        }
                                    } else {
                                        System.out.println("Error: Failed to execute simulation step");
                                    }
                                }
                            } else if (msg.hasStopSimulation()) {
                                System.out.println("model time " + engine.time() + " : received STOP_SIMULATION");
                                owner.stop();
                            }
                        } catch (Exception e) {
                            System.out.println("Error parsing command: " + e.getMessage());
                        }
                    }
                } catch (WakeupException | InterruptException e) {
                    break;
                } catch (Exception e) {
                    if (!owner.isRunning()) break;
                    System.err.print("Command thread error: " + e.getMessage());
                }
            }
            Thread.interrupted();
            try { cmdConsumer.close(); } catch (Exception ignored) {}
        });
        commandThread.setDaemon(true);
        commandThread.start();

        // ==========================================
        // 2. CONSUMER THREAD (Прием агентов)
        // ==========================================
        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, owner.getID() + "-agents");
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(consProps);
        consumer.subscribe(Collections.singletonList(owner.getInboxTopic()));

        this.consumerThread = new Thread(() -> {
            while (owner.isRunning()) {
                try {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                    for (var record : records) {
                        var msg = Protobufmessages.OrchestratorMessage.parseFrom(record.value());
                        if (msg.hasAgentTransfer()) {
                            byte[] agentBytes = msg.getAgentTransfer().getAgent().toByteArray();
                            // Кладем задачу в очередь, она выполнится на старте следующего шага
                            owner.tasksFromKafka.add(() -> {
                                if (owner.receivingAgent != null) {
                                    owner.receivingAgent.receiveAgentFromOrchestrator(agentBytes);
                                }
                            });
                        }
                    }
                } catch (WakeupException | InterruptException e) {
                    break;
                } catch (Exception e) {
                    if (!owner.isRunning()) break;
                    System.err.print("Consumer thread error: " + e.getMessage());
                }
            }
            Thread.interrupted();
            try { consumer.close(); } catch (Exception ignored) {}
        });
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    public void stop() {
        owner.setRunning(false);
        if (cmdConsumer != null) try { cmdConsumer.wakeup(); } catch (Exception ignored) {}
        if (consumer != null) try { consumer.wakeup(); } catch (Exception ignored) {}
        try {
            if (commandThread != null) commandThread.join(3000);
            if (consumerThread != null) consumerThread.join(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}