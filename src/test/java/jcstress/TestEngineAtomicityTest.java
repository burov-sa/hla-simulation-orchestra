package jcstress;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.II_Result;
import ru.itmo.infrastructure.TestEngine;

//Проверка, что операция инкремента
// в AtomicInteger является атомарной.
// Два потока одновременно вызывают incrementAndGet(),
// и мы проверяем, что итоговое значение всегда равно 2,
// и нет потеряшек.

//enum Expect {ACCEPTABLE - норм, ACCEPTABLE_INTERESTING- допустимо, FORBIDDEN - не должно так быть, UNKNOWN - неизветный исход
@JCStressTest
@Outcome(id = "0, 0", expect = Expect.ACCEPTABLE,               desc = "Оба потока прочитали 0 (до первого инкремента) - норм")
@Outcome(id = "0, 1", expect = Expect.ACCEPTABLE_INTERESTING,   desc = "Actor1 прочитал 0, Actor2 - 1")
@Outcome(id = "1, 0", expect = Expect.ACCEPTABLE_INTERESTING,   desc = "Actor2 прочитал 0, Actor1 - 1")
//@Outcome возможные ситуации
@State //экз.класса общий для всех потомков-акторов
public class TestEngineAtomicityTest {

    private final TestEngine engine = new TestEngine(0.0, 100.0, 0);

    @Actor//каждый актор выполняется в отдельном потоке
    public void actor1(II_Result r) {
        r.r1 = engine.getRunCount(); // Читаем начальное значение (должно быть 0)
        engine.run();                // Увеличиваем счётчик
    }

    @Actor
    public void actor2(II_Result r) {
        r.r2 = engine.getRunCount(); // Читаем значение после возможного инкремента
        engine.run();
    }
}