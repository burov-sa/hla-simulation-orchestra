package ru.itmo.model;

// ИСПРАВЛЕНО: Заменили импорты Jackson на компоненты Gson
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonPrimitive;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.time.temporal.ChronoUnit;

/**
 * Загрузчик конфигурации моделирования из JSON-файла.
 * Ожидает структуру с общими настройками и списком моделей.
 */
public class SimulationConfigLoader {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ss");

    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(LocalDateTime.class, (JsonSerializer<LocalDateTime>) (src, typeOfSrc, context) ->
                    new JsonPrimitive(src.format(DATE_TIME_FORMATTER)))
            .registerTypeAdapter(LocalDateTime.class, (JsonDeserializer<LocalDateTime>) (json, typeOfT, context) ->
                    LocalDateTime.parse(json.getAsString(), DATE_TIME_FORMATTER))
            .create();

    /**
     * Загружает список конфигураций {@link SimulationConfig} из указанного JSON-файла.
     *
     * @param filePath путь к файлу конфигурации (например, "config.json")
     * @return список сконфигурированных объектов SimulationConfig
     * @throws IOException если файл не найден или не может быть прочитан
     */
    public static List<SimulationConfig> loadFromJson(String filePath) throws IOException {
        try (FileReader reader = new FileReader(filePath, StandardCharsets.UTF_8)) {
            SimulationConfigFile configFile = gson.fromJson(reader, SimulationConfigFile.class);

            return configFile.models.stream()
                    .map(m -> new SimulationConfig(
                            m.id,
                            m.filePath,
                            configFile.timeUnit,
                            configFile.startSimulation,
                            configFile.stopSimulation,
                            configFile.isDebug
                    ))
                    .collect(Collectors.toList());
        }
    }

    /**
     * Вспомогательный класс, зеркально отражающий структуру JSON-файла.
     */
    public static class SimulationConfigFile {
        public ChronoUnit timeUnit;
        public LocalDateTime startSimulation;
        public LocalDateTime stopSimulation;
        public boolean isDebug;
        public List<SimulationConfigFile.ModelEntry> models;

        public static class ModelEntry {
            public String id;
            public String filePath;
        }
    }
}
