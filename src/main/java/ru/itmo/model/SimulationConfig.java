package ru.itmo.model;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.AllArgsConstructor;

/**
 * Класс для хранения конфигурации моделирования для каждой модели
 *
 */
@Getter
@Setter
@AllArgsConstructor
public class SimulationConfig {
    final String id; //id модели
    final String filePath; //путь к исполняемому файлу
    final ChronoUnit timeUnit; //единицы модельного времени
    final LocalDateTime startSimulation; //дата и время начала моделирования
    final LocalDateTime stopSimulation; //дата и время окончания моделирования
    final boolean isDebug; //признак режима отладки при моделировании

    @Override
    public String toString() {
        return "SimulationConfig<id=" + this.id +
                ", filePath=" + this.filePath +
                ", timeUnit=" + this.timeUnit +
                ", startSimulation=" + this.startSimulation +
                ", stopSimulation=" + this.stopSimulation +
                ", isDebug=" + this.isDebug() + ">";
    }
}