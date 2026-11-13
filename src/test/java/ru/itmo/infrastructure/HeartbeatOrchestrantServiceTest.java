package ru.itmo.infrastructure;

import com.google.protobuf.InvalidProtocolBufferException;
import infrastructure.Protobufmessages;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
@Tag("integration-test")
class HeartbeatOrchestrantServiceTest {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String HEARTBEAT_TOPIC = "model-heartbeat";
    private static final String MODEL_ID = "test-heartbeat-model";

    @Test
    void heartbeatService_sendsMessageEveryInterval() throws Exception {
        KafkaOrchestrant mockOwner = new KafkaOrchestrant(MODEL_ID, null, null, false);

        // Настраиваем продюсер
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        KafkaProducer<String, byte[]> realProducer = new KafkaProducer<>(prodProps);

        Field producerField = KafkaOrchestrant.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(mockOwner, realProducer);

        Field runningField = KafkaOrchestrant.class.getDeclaredField("running");
        runningField.setAccessible(true);
        ((AtomicBoolean) runningField.get(mockOwner)).set(true);

        // Сначала создаём консьюмер и подписываемся, чтобы не пропустить сообщение
        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-heartbeat-" + UUID.randomUUID());
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // ВАЖНО: читаем только новые сообщения, игнорируя старые (возможно, не Protobuf)
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consProps)) {
            consumer.subscribe(Collections.singletonList(HEARTBEAT_TOPIC));

            // Ждём, пока консьюмер получит назначение партиций
            long assignmentDeadline = System.currentTimeMillis() + 5000;
            while (consumer.assignment().isEmpty() && System.currentTimeMillis() < assignmentDeadline) {
                consumer.poll(Duration.ofMillis(200));
            }
            assertFalse(consumer.assignment().isEmpty(), "Consumer did not get partition assignment");

            // Теперь запускаем сервис, который начнёт отправлять heartbeat через 10 секунд
            HeartbeatOrchestrantService service = new HeartbeatOrchestrantService(mockOwner);
            service.start();

            try {
                long deadline = System.currentTimeMillis() + 15_000;
                boolean received = false;

                while (System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, byte[]> record : records) {
                        try {
                            var msg = Protobufmessages.ModelMessage.parseFrom(record.value());
                            if (msg.getKindCase() == Protobufmessages.ModelMessage.KindCase.HEARTBEAT
                                    && MODEL_ID.equals(msg.getSenderModelId())) {
                                received = true;
                                break;
                            }
                        } catch (InvalidProtocolBufferException e) {
                            // Игнорируем старые сообщения, которые не являются Protobuf
                            // (на случай, если они всё же попали в consumer)
                        }
                    }
                    if (received) break;
                }

                assertTrue(received, "Heartbeat message was not received within 15 seconds");
            } finally {
                service.stop();
                realProducer.close();
            }
        }
    }
}