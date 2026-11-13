package ru.itmo.utils;

import ru.itmo.model.SimulationConfig;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * Класс для работы с командной строкой
 * (получение файлов конфигурации из командной строки и т.д.)
 */
public class CommandLineService {

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ss");

    public static SimulationConfig loadConfigFromCommandLine(String[] settings) {
        try {
            // Ожидаем либо 4 аргумента: modelId timeUnit startDate stopDate
            // либо 5 аргументов: isDebug modelId timeUnit startDate stopDate
            if (settings == null || (settings.length != 4 && settings.length != 5)) {
                System.out.println("Error: expected 4 or 5 command line arguments, got "
                        + (settings == null ? "null" : settings.length));
                return null;
            }

            int index = 0;

            boolean isDebug = false;
            if (settings.length == 5) {
                isDebug = Boolean.parseBoolean(settings[index++]);
            }

            String id = settings[index++];
            ChronoUnit timeUnit = ChronoUnit.valueOf(settings[index++]);
            LocalDateTime startSimulation = LocalDateTime.parse(settings[index++], DATE_TIME_FORMATTER);
            LocalDateTime stopSimulation = LocalDateTime.parse(settings[index++], DATE_TIME_FORMATTER);

            String filePath = ""; // путь к исполняемому файлу при запуске из командной строки не передаётся

            return new SimulationConfig(
                    id,
                    filePath,
                    timeUnit,
                    startSimulation,
                    stopSimulation,
                    isDebug
            );

        } catch (Exception e) {
            System.out.println("Error: unexpected model launch error during loading configuration from command line: "
                    + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}