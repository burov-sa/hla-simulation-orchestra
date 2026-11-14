package ru.itmo.utils;

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import lombok.extern.log4j.Log4j2;

/**
 * Класс TeeOutputStream представляет собой Т-образный разветвитель потока вывода.
 * Он дублирует стандартную консоль в файл и поддерживает автоматическую инициализацию.
 */
@Log4j2
public class TeeOutputStreamService extends OutputStream {

    private final OutputStream out1;
    private final OutputStream out2;

    /**
     * Конструктор разветвителя потоков.
     */
    public TeeOutputStreamService(OutputStream out1, OutputStream out2) {
        this.out1 = out1;
        this.out2 = out2;
    }

    /**
     * Статичный метод для быстрой активации перехвата консоли.
     *
     * @param fileName Путь к файлу лога (например, "model_log.txt")
     * @param append   Если true — логи дописываются в конец файла.
     *                 Если false — файл очищается при каждом новом запуске.
     */
    public static void initInterceptor(String fileName, boolean append) {
        try {
            // Создаем поток записи в файл с учетом флага append
            FileOutputStream fos = new FileOutputStream(fileName, append);

            // Создаем разветвленные потоки для System.out и System.err
            // Включаем auto-flush (true), чтобы данные сразу сбрасывались на диск
            PrintStream teeOut = new PrintStream(new TeeOutputStreamService(System.out, fos), true, StandardCharsets.UTF_8);
            PrintStream teeErr = new PrintStream(new TeeOutputStreamService(System.err, fos), true, StandardCharsets.UTF_8);

            // Перенаправляем стандартные потоки Java
            System.setOut(teeOut);
            System.setErr(teeErr);

            String modeMessage = append ? "дописание в конец" : "очистка файла";
            log.info("Перехват консоли запущен (Режим: {}) ---", modeMessage);
        } catch (Exception e) {
            log.error("Error: error initianalization interrupt console", e);
        }
    }

    @Override
    public void write(int b) throws IOException {
        out1.write(b);
        out2.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        out1.write(b);
        out2.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out1.write(b, off, len);
        out2.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        out1.flush();
        out2.flush();
    }

    @Override
    public void close() throws IOException {
        try {
            out1.close();
        } finally {
            out2.close();
        }
    }
}

