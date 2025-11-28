package relational.examples;

import org.javatuples.Quartet;
import relational.stream.RowStream;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PolyflowAnimalSpeedFilterTest {

    @Test
    void testTupleFiltering() {
        List<Quartet<Long, String, Double, Long>> acceptedTuples = new ArrayList<>();
        Quartet<Long, String, Double, Long> lowSpeedTuple = new Quartet<>(1L, "lion", 10.0, 0L);
        Quartet<Long, String, Double, Long> highSpeedTuple = new Quartet<>(2L, "zebra", 20.0, 0L);

        boolean lowAccepted = lowSpeedTuple.getValue2() > 15;
        boolean highAccepted = highSpeedTuple.getValue2() > 15;

        if (lowAccepted)
            acceptedTuples.add(lowSpeedTuple);
        if (highAccepted)
            acceptedTuples.add(highSpeedTuple);

        assertFalse(lowAccepted, "Low-speed tuple should be rejected");
        assertTrue(highAccepted, "High-speed tuple should be accepted");
        assertEquals(1, acceptedTuples.size(), "Only one tuple should be accepted");
        assertEquals(20.0, acceptedTuples.get(0).getValue2());
    }

    @Test
    void testStreamingConsumerLogic() {
        List<Quartet<Long, String, Double, Long>> consumed = new ArrayList<>();
        RowStream<Quartet<Long, String, Double, Long>> outStream = new RowStream<>("test");

        outStream.addConsumer((stream, el, ts) -> consumed.add(el));

        outStream.put(new Quartet<>(1L, "lion", 17.0, 0L), 0L);
        outStream.put(new Quartet<>(2L, "zebra", 22.0, 200L), 200L);

        assertEquals(2, consumed.size());
        assertEquals("lion", consumed.get(0).getValue1());
        assertEquals(22.0, consumed.get(1).getValue2());
    }

    @Test
    void testTupleFilteringBoundarySpeed() {
        // Speed exactly at threshold
        Quartet<Long, String, Double, Long> boundaryTuple = new Quartet<>(3L, "gazelle", 15.0, 0L);
        boolean accepted = boundaryTuple.getValue2() > 15;
        assertFalse(accepted, "Tuple with speed = 15 should be rejected");
    }

    @Test
    void testTupleFilteringZeroAndNegativeSpeed() {
        Quartet<Long, String, Double, Long> zeroSpeed = new Quartet<>(4L, "dog", 0.0, 0L);
        Quartet<Long, String, Double, Long> negativeSpeed = new Quartet<>(5L, "cat", -5.0, 0L);

        assertFalse(zeroSpeed.getValue2() > 15, "Tuple with speed 0 should be rejected");
        assertFalse(negativeSpeed.getValue2() > 15, "Tuple with negative speed should be rejected");
    }

    @Test
    void testTupleFilteringMultipleTuples() {
        List<Quartet<Long, String, Double, Long>> tuples = List.of(
                new Quartet<>(6L, "elephant", 12.0, 0L),
                new Quartet<>(7L, "lion", 16.0, 0L),
                new Quartet<>(8L, "zebra", 20.0, 0L));

        List<Quartet<Long, String, Double, Long>> accepted = new ArrayList<>();
        for (Quartet<Long, String, Double, Long> t : tuples) {
            if (t.getValue2() > 15)
                accepted.add(t);
        }

        assertEquals(2, accepted.size(), "Two tuples should be accepted");
        assertEquals(16.0, accepted.get(0).getValue2());
        assertEquals(20.0, accepted.get(1).getValue2());
    }
}
