package ru.itmo.infrastructure;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TestEngine implements IEngine {

    private volatile double scheduledEndTime = -1;
    private final double startTime;
    private final double stopTime;
    private final AtomicReference<Double> currentTime;

    private volatile boolean isRunning = false;
    private volatile boolean isPaused = false;
    private final long simulationDelayMs;

    // Счетчики вызовов
    private final AtomicInteger runCount = new AtomicInteger(0);
    private final AtomicInteger runFastCount = new AtomicInteger(0);
    private final AtomicInteger pauseCount = new AtomicInteger(0);
    private final AtomicInteger stopCount = new AtomicInteger(0);
    private final AtomicInteger experimentCloseCount = new AtomicInteger(0);

    // Счетчики и данные событий
    private final AtomicInteger firstStepEventCount = new AtomicInteger(0);
    private final AtomicInteger stepStartEventCount = new AtomicInteger(0);
    private final AtomicInteger stepEndEventCount = new AtomicInteger(0);

    private final AtomicReference<Double> lastStepEndDuration = new AtomicReference<>();

    public TestEngine() {
        this(0.0, 100.0, 50);
    }

    public TestEngine(double startTime, double stopTime, long simulationDelayMs) {
        this.startTime = startTime;
        this.stopTime = stopTime;
        this.simulationDelayMs = simulationDelayMs;
        this.currentTime = new AtomicReference<>(startTime);
    }

    @Override public double time() { return currentTime.get(); }
    @Override public double getStartTime() { return startTime; }
    @Override public double getStopTime() { return stopTime; }

    @Override
    public boolean run() {
        runCount.incrementAndGet();
        System.out.println("[TestEngine] run() called. Time: " + time());
        if (isRunning) return false;
        isRunning = true;
        isPaused = false;
        simulateWork();
        return true;
    }

    @Override
    public boolean runFast() {
        runFastCount.incrementAndGet();
        System.out.println("[TestEngine] runFast() called. Time: " + time());
        if (isRunning) return false;
        isRunning = true;
        isPaused = false;

        simulateWork(); // "Выполняем" работу

        // Эмуляция работы AnyLogic: движок доходит до запланированного события
        if (scheduledEndTime >= 0) {
            currentTime.set(scheduledEndTime);
            scheduledEndTime = -1;
        }

        isRunning = false;
        isPaused = true;
        return true;
    }

    @Override
    public boolean pause() {
        pauseCount.incrementAndGet();
        System.out.println("[TestEngine] pause() called. Time: " + time());
        if (!isRunning && !isPaused) return false;
        isRunning = false;
        isPaused = true;
        return true;
    }

    @Override
    public boolean stop() {
        stopCount.incrementAndGet();
        System.out.println("[TestEngine] stop() called.");
        if (!isRunning && !isPaused) return false;
        isRunning = false;
        isPaused = false;
        return true;
    }

    @Override
    public boolean experimentClose() {
        experimentCloseCount.incrementAndGet();
        return true;
    }

    @Override
    public void executeFirstStepEvent() {
        firstStepEventCount.incrementAndGet();
        System.out.println("[TestEngine] FirstStepEvent: model is paused, waiting for NEXT_STEP...");
        this.isRunning = false;
        this.isPaused = true;
    }

    @Override
    public void executeStepStartEvent() {
        stepStartEventCount.incrementAndGet();
        System.out.println("[TestEngine] StepStartEvent. ");
    }

    @Override
    public void executeStepEndEvent(double duration) {
        stepEndEventCount.incrementAndGet();
        lastStepEndDuration.set(duration);
        System.out.println("[TestEngine] StepEndEvent. Duration: " + duration);
        // Планируем, до какого времени движок должен доработать
        scheduledEndTime = currentTime.get() + duration;
    }

    // Геттеры
    public int getRunCount() { return runCount.get(); }
    public int getRunFastCount() { return runFastCount.get(); }
    public int getPauseCount() { return pauseCount.get(); }
    public int getStopCount() { return stopCount.get(); }
    public int getFirstStepEventCount() { return firstStepEventCount.get(); }
    public int getStepStartEventCount() { return stepStartEventCount.get(); }
    public int getStepEndEventCount() { return stepEndEventCount.get(); }
    public Double getLastStepEndDuration() { return lastStepEndDuration.get(); }

    public void setTime(double newTime) { currentTime.set(newTime); }
    public void advanceTime(double delta) { currentTime.updateAndGet(v -> v + delta); }

    private void simulateWork() {
        try {
            Thread.sleep(simulationDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}