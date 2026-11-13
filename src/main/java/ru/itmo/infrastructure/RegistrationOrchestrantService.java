package ru.itmo.infrastructure;

import infrastructure.Protobufmessages;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RegistrationOrchestrantService {
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
        } catch (Exception e) { System.err.print("Error ONLINE: " + e.getMessage()); }
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
        } catch (Exception e) { System.err.print("Error OFFLINE: " + e.getMessage()); }
    }
}