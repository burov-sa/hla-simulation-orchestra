package jcstress;

import org.openjdk.jcstress.annotations.*;
import ru.itmo.infrastructure.KafkaOrchestrant;

@JCStressTest(Mode.Termination)
@Outcome(
        id = "TERMINATED",
        expect = Expect.ACCEPTABLE,
        desc = "Actor 1 увидел running = false и вышел из цикла — видимость работает"
)
@Outcome(
        id = "STALE",
        expect = Expect.FORBIDDEN,
        desc = "Actor 1 не увидел running = false и застрял в бесконечном цикле — баг видимости"
)
@Outcome(
        id = "STILL_RUNNING",
        expect = Expect.ACCEPTABLE_INTERESTING,
        desc = "Actor 1 ещё работает (не успел проверить флаг) — не баг, но шумный исход"
)
@State
public class KafkaOrchestrantVisibilityTest {

    private final KafkaOrchestrant orchestrant;

    public KafkaOrchestrantVisibilityTest() {
        this.orchestrant = new KafkaOrchestrant("test-model", null, null, false);
        this.orchestrant.setRunning(true);
    }

    /**
     * Единственный актор, который мы проверяем на зависание.
     * Он будет крутиться в цикле, пока running = true.
     */
    @Actor
    public void actor1_reader() {
        while (orchestrant.isRunning()) {
            // busy-wait: ждем, когда другой поток выставит false
        }
    }

    /**
     * В режиме Termination этот метод вызывается, когда @Actor уже запущен.
     * Именно здесь мы должны "отпустить" актор, чтобы проверить, увидит ли он изменение.
     */
    @Signal
    public void signal_writer() {
        orchestrant.setRunning(false);
    }
}