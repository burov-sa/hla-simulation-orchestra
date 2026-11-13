package ru.itmo.infrastructure;

/**
 * Функциональный интерфейс процесс отправки агента внешним моделям
 * @agent - объект TransferAgent
 * @idDestination - id внешней модели
 */
@FunctionalInterface
public interface ISendingAgent {
    public void sendAgentToOrchestrator(TransferObject agent, String idDestination);
}
