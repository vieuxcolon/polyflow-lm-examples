package relational.animal_examples;

import org.javatuples.Quartet;
import relational.stream.RowStream;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PolyflowAnimalSpeedThresholdFrameTest {

    private static final double THRESHOLD = 15.0;

    @Test
    void testSingleThresholdFrame() {
        RowStream<Quartet<Long, String, Double, Long>> outStream = new RowStream<>("out");
        List<Quartet<Long, String, Double, Long>> emitted = new ArrayList<>();
        outStream.addConsumer((stream, tuple, ts) -> emitted.add(tuple));

        // Example tuples
        Quartet<Long, String, Double, Long> t1 = new Quartet<>(1L, "lion", 16.0, 0L); // above threshold
        Quartet<Long, String, Double, Long> t2 = new Quartet<>(2L, "zebra", 18.0, 100L); // above threshold
        Quartet<Long, String, Double, Long> t3 = new Quartet<>(3L, "gazelle", 14.0, 200L); // below threshold

        // Simulate threshold-frame logic
        List<Quartet<Long, String, Double, Long>> frame = new ArrayList<>();
        for (Quartet<Long, String, Double, Long> t : List.of(t1, t2, t3)) {
            if (t.getValue2() >= THRESHOLD) {
                frame.add(t);
            } else if (!frame.isEmpty()) {
                for (Quartet<Long, String, Double, Long> f : frame) {
                    outStream.put(f, f.getValue3());
                }
                frame.clear();
            }
        }

        assertEquals(2, emitted.size());
        assertEquals("lion", emitted.get(0).getValue1());
        assertEquals("zebra", emitted.get(1).getValue1());
    }

    @Test
    void testMultipleFrames() {
        RowStream<Quartet<Long, String, Double, Long>> outStream = new RowStream<>("out");
        List<Quartet<Long, String, Double, Long>> emitted = new ArrayList<>();
        outStream.addConsumer((stream, tuple, ts) -> emitted.add(tuple));

        // Alternating tuples
        List<Quartet<Long, String, Double, Long>> tuples = List.of(
                new Quartet<>(1L, "lion", 16.0, 0L),
                new Quartet<>(2L, "zebra", 14.0, 100L),
                new Quartet<>(3L, "gazelle", 17.0, 200L),
                new Quartet<>(4L, "dog", 13.0, 300L),
                new Quartet<>(5L, "cat", 20.0, 400L));

        List<Quartet<Long, String, Double, Long>> frame = new ArrayList<>();
        for (Quartet<Long, String, Double, Long> t : tuples) {
            if (t.getValue2() >= THRESHOLD)
                frame.add(t);
            else if (!frame.isEmpty()) {
                for (Quartet<Long, String, Double, Long> f : frame)
                    outStream.put(f, f.getValue3());
                frame.clear();
            }
        }
        // flush remaining frame
        for (Quartet<Long, String, Double, Long> f : frame)
            outStream.put(f, f.getValue3());

        assertEquals(3, emitted.size());
        assertEquals(List.of(1L, 3L, 5L), List.of(
                emitted.get(0).getValue0(),
                emitted.get(1).getValue0(),
                emitted.get(2).getValue0()));
    }

    @Test
    void testBoundaryThreshold() {
        RowStream<Quartet<Long, String, Double, Long>> outStream = new RowStream<>("out");
        List<Quartet<Long, String, Double, Long>> emitted = new ArrayList<>();
        outStream.addConsumer((stream, tuple, ts) -> emitted.add(tuple));

        Quartet<Long, String, Double, Long> t1 = new Quartet<>(1L, "gazelle", THRESHOLD, 0L);

        List<Quartet<Long, String, Double, Long>> frame = new ArrayList<>();
        if (t1.getValue2() >= THRESHOLD)
            frame.add(t1);
        for (Quartet<Long, String, Double, Long> f : frame)
            outStream.put(f, f.getValue3());

        assertEquals(1, emitted.size());
        assertEquals(THRESHOLD, emitted.get(0).getValue2());
    }

    @Test
    void testZeroAndNegativeSpeeds() {
        RowStream<Quartet<Long, String, Double, Long>> outStream = new RowStream<>("out");
        List<Quartet<Long, String, Double, Long>> emitted = new ArrayList<>();
        outStream.addConsumer((stream, tuple, ts) -> emitted.add(tuple));

        Quartet<Long, String, Double, Long> t1 = new Quartet<>(1L, "dog", 0.0, 0L);
        Quartet<Long, String, Double, Long> t2 = new Quartet<>(2L, "cat", -5.0, 100L);

        List<Quartet<Long, String, Double, Long>> frame = new ArrayList<>();
        for (Quartet<Long, String, Double, Long> t : List.of(t1, t2))
            if (t.getValue2() >= THRESHOLD)
                frame.add(t);
        for (Quartet<Long, String, Double, Long> f : frame)
            outStream.put(f, f.getValue3());

        assertTrue(emitted.isEmpty(), "No tuples should be emitted for zero or negative speeds");
    }

    @Test
    void testLongSequenceWithMultipleFrames() {
        RowStream<Quartet<Long, String, Double, Long>> outStream = new RowStream<>("out");
        List<Quartet<Long, String, Double, Long>> emitted = new ArrayList<>();
        outStream.addConsumer((stream, tuple, ts) -> emitted.add(tuple));

        List<Quartet<Long, String, Double, Long>> tuples = new ArrayList<>();
        for (long i = 1; i <= 10; i++) {
            double speed = (i % 3 == 0) ? 10.0 : 20.0; // every third tuple below threshold
            tuples.add(new Quartet<>(i, "animal" + i, speed, i * 100L));
        }

        List<Quartet<Long, String, Double, Long>> frame = new ArrayList<>();
        for (Quartet<Long, String, Double, Long> t : tuples) {
            if (t.getValue2() >= THRESHOLD)
                frame.add(t);
            else if (!frame.isEmpty()) {
                for (Quartet<Long, String, Double, Long> f : frame)
                    outStream.put(f, f.getValue3());
                frame.clear();
            }
        }
        for (Quartet<Long, String, Double, Long> f : frame)
            outStream.put(f, f.getValue3());

        // Should emit 7 tuples: sequences of 2, 2, 3 above threshold
        assertEquals(7, emitted.size());
    }
}
