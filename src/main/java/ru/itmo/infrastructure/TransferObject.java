package ru.itmo.infrastructure;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DTO для обмена между моделями
 * Agent --> TransferObject --> ProtobuffMessages.AgentMessage --> TransferObject --> Agent
 */

@AllArgsConstructor
@Getter
public class TransferObject {
    private final int agentId;      // id агента в источнике
    private final int sourceId;	    // id источника возникновения заказа
    private double creationTime; 	// время возникновения заказа в единицах модельного времени
    private int categoryId; 	    // id категории заказа
    private int categoryFlowId;     // id категории потока
    private double volume; 		    // объём или при необходимости вес заказа
}
