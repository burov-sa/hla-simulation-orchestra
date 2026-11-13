package ru.itmo.infrastructure;

/**
 * Функциональный интерфейс процесс получения агента от внешних моделей
 * @parameter date - байтовый массив с сообщением
 */
@FunctionalInterface
public interface IReceivingAgent
{
    public void receiveAgentFromOrchestrator( byte[] data);
}
