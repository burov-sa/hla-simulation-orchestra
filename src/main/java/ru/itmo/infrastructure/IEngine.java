package ru.itmo.infrastructure;

/**
 * Интерфейс основного потока модели оркестра
 */
public interface IEngine {
    /**
     *
     * @return текущее модельное время
     */
    public double time();

    /**
     *
     * @return время начала моделирования в единицах модельного времени
     */
    public double getStartTime();

    /**
     *
     * @return время окончания моделирования в единицах модельного времени
     */
    public double getStopTime();

    /**
     *
     * @return true - модель была в состоянии паузы и продолжила симуляцию
     */
    public boolean run();

    /**
     *
     * @return true - модель была в состоянии паузы и продолжила ускоренную симуляцию
     */
    public boolean runFast();

    /**
     *
     * @return true - симуляция на паузе
     */
    public boolean pause();

    /**
     *
     * @return true - симуляция завершена
     */
    public boolean stop();

    /**
     *
     * @return true - успешное завершение эксперимента
     */
    public boolean experimentClose();

    /**
     * Вызывается один раз в начале симуляции.
     * Внутри, например AnyLogic, должно планироваться DynamicEvent на время 0,
     * который ставит модель на паузу и переводит в режим ожидания.
     *
     */
    void executeFirstStepEvent();

    /**
     * Вызывается перед началом нового шага (команда NEXT_STEP).
     * Внутри AnyLogic может планировать DynamicEvent, если нужно.
     */
    void executeStepStartEvent();

    /**
     * Вызывается в конце шага (перед тем, как движок сделает паузу).
     * Внутри AnyLogic планирует DynamicEvent, чтобы гарантированно выполнить действия.
     * @param duration длительность шага.
     */
    void executeStepEndEvent(double duration);

}
