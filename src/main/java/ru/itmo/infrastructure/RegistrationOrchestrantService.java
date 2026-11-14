package ru.itmo.infrastructure;

import infrastructure.Protobufmessages;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;

@Log4j2
public final class RegistrationOrchestrantService {
    KafkaOrchestrant owner;

    public RegistrationOrchestrantService(KafkaOrchestrant owner) { this.owner = owner; }

    public void subscribe() {
        try {
            var reg = Protobufmessages.Registration.newBuilder()
                    .setInboxTopic(owner.getInboxTopic())
                    .setIsOnline(true)
                    .build();
            var msg = Protobufmessages.ModelMessage.newBuilder()
                    .setSenderModelId(owner.getID())
                    .setRegistration(reg)
                    .build();
            owner.getProducer().send(new ProducerRecord<>(owner.getModelRegistyTopic(), msg.toByteArray()));
        } catch (Exception e) {
            log.error("ONLINE", e);
        }
    }

    public void unsubscribe() {
        try {
            var reg = Protobufmessages.Registration.newBuilder()
                    .setIsOnline(false)
                    .build();
            var msg = Protobufmessages.ModelMessage.newBuilder()
                    .setSenderModelId(owner.getID())
                    .setRegistration(reg)
                    .build();
            owner.getProducer().send(new ProducerRecord<>(owner.getModelRegistyTopic(), msg.toByteArray()));
            owner.getProducer().flush();
        } catch (Exception e) {
            log.error("OFFLINE", e);
        }
    }
}