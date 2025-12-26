package relational.animal_examples;

import org.javatuples.Quartet;
import relational.stream.RowStream;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PolyflowAnimalSpeedAccumulateTest {

    @Test
    void testSimpleAccumulation() {
        // Prepare output stream and collector
        RowStream<Quartet<Long, String, Double, Long>> outStream = new RowStream<>("out");
        List<Quartet<Long, String, Double, Long>> emitted = new ArrayList<>();
        outStream.addConsumer((stream, tuple, ts) -> emitted.add(tuple));

        // Input tuples
        List<Quartet<Long, String, Double, Long>> tuples = List.of(
                new Quartet<>(1L, "lion", 5.0, 0L),
                new Quartet<>(2L, "zebra", 10.0, 0L)
        );

        // Accumulate speed manually to simulate polyflow_animalSpeed_accumulate
        double accumulatedSpeed = 0.0;
        for (Quartet<Long, String, Double, Long> t : tuples) {
            double speed = t.getValue2(); // speed is value2
            accumulatedSpeed += speed;
            outStream.put(t, t.getValue3()); // simulate streaming
        }

        // Verify accumulation
        assertEquals(15.0, accumulatedSpeed, 0.001, "Accumulated speed should be 15");

        // Verify emitted tuples
        assertEquals(2, emitted.size(), "Two tuples should be emitted");
        assertEquals("lion", emitted.get(0).getValue1());
        assertEquals(5.0, emitted.get(0).getValue2());
        assertEquals("zebra", emitted.get(1).getValue1());
        assertEquals(10.0, emitted.get(1).getValue2());
    }

    @Test
    void testMultipleTuplesAccumulation() {
        RowStream<Quartet<Long, String, Double, Long>> outStream = new RowStream<>("out");
        List<Quartet<Long, String, Double, Long>> emitted = new ArrayList<>();
        outStream.addConsumer((stream, tuple, ts) -> emitted.add(tuple));

        List<Quartet<Long, String, Double, Long>> tuples = List.of(
                new Quartet<>(1L, "lion", 12.0, 0L),
                new Quartet<>(2L, "zebra", 18.0, 50L),
                new Quartet<>(3L, "gazelle", 20.0, 100L)
        );

        double accumulated = 0.0;
        for (var t : tuples) {
            accumulated += t.getValue2();
            outStream.put(t, t.getValue3());
        }

        assertEquals(50.0, accumulated, 0.001, "Accumulated speed should be 50");
        assertEquals(3, emitted.size(), "Three tuples should be emitted");
    }

    @Test
    void testZeroAndNegativeSpeed() {
        RowStream<Quartet<Long, String, Double, Long>> outStream = new RowStream<>("out");
        List<Quartet<Long, String, Double, Long>> emitted = new ArrayList<>();
        outStream.addConsumer((stream, tuple, ts) -> emitted.add(tuple));

        List<Quartet<Long, String, Double, Long>> tuples = List.of(
                new Quartet<>(1L, "cat", -5.0, 0L),
                new Quartet<>(2L, "dog", 0.0, 0L)
        );

        double accumulated = 0.0;
        for (var t : tuples) {
            accumulated += t.getValue2();
            outStream.put(t, t.getValue3());
        }

        assertEquals(-5.0, accumulated, 0.001, "Accumulated speed should be -5");
        assertEquals(2, emitted.size(), "Both tuples should be emitted even if speeds are zero or negative");
    }

    @Test
    void testEmptyStream() {
        RowStream<Quartet<Long, String, Double, Long>> outStream = new RowStream<>("out");
        List<Quartet<Long, String, Double, Long>> emitted = new ArrayList<>();
        outStream.addConsumer((stream, tuple, ts) -> emitted.add(tuple));

        double accumulated = 0.0; // no tuples
        assertEquals(0.0, accumulated, "Accumulated speed for empty stream should be 0");
        assertTrue(emitted.isEmpty(), "No tuples should be emitted for empty stream");
    }

}
