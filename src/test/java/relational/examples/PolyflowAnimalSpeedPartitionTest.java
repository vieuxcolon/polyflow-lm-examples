package relational.animal_examples;

import org.javatuples.Quartet;
import relational.stream.RowStream;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PolyflowAnimalSpeedPartitionTest {

    @Test
    void testPartitionAssignment() {
        // Simulate animal tuples
        List<Quartet<Long, String, Double, Long>> tuples = List.of(
                new Quartet<>(1L, "lion", 12.0, 0L),
                new Quartet<>(2L, "zebra", 18.0, 0L),
                new Quartet<>(3L, "gazelle", 22.0, 0L),
                new Quartet<>(4L, "elephant", 10.0, 0L));

        List<Integer> partitions = new ArrayList<>();
        for (Quartet<Long, String, Double, Long> t : tuples) {
            int partition = (int) (t.getValue0() % 3);
            partitions.add(partition);
        }

        assertEquals(List.of(1, 2, 0, 1), partitions, "Partition assignment should match ID % 3");
    }

    @Test
    void testTupleAggregationIntoTable() {
        // Simulate accumulation into a Table
        List<Quartet<Long, String, Double, Long>> tuples = List.of(
                new Quartet<>(1L, "lion", 12.0, 0L),
                new Quartet<>(2L, "zebra", 18.0, 0L));

        tech.tablesaw.api.Table table = tech.tablesaw.api.Table.create("animal_data");
        table.addColumns(
                tech.tablesaw.api.LongColumn.create("animalId"),
                tech.tablesaw.api.StringColumn.create("species"),
                tech.tablesaw.api.DoubleColumn.create("speed"));

        for (Quartet<Long, String, Double, Long> t : tuples) {
            table.longColumn("animalId").append(t.getValue0());
            table.stringColumn("species").append(t.getValue1());
            table.doubleColumn("speed").append(t.getValue2());
        }

        assertEquals(2, table.rowCount());
        assertEquals("lion", table.stringColumn("species").get(0));
        assertEquals(18.0, table.doubleColumn("speed").get(1));
    }

    @Test
    void testStreamingConsumer() {
        RowStream<Quartet<Long, String, Double, Long>> outStream = new RowStream<>("out");
        List<Quartet<Long, String, Double, Long>> received = new ArrayList<>();

        outStream.addConsumer((stream, tuple, ts) -> received.add(tuple));

        Quartet<Long, String, Double, Long> t1 = new Quartet<>(1L, "lion", 12.0, 0L);
        Quartet<Long, String, Double, Long> t2 = new Quartet<>(2L, "zebra", 18.0, 0L);

        outStream.put(t1, t1.getValue3());
        outStream.put(t2, t2.getValue3());

        assertEquals(2, received.size());
        assertEquals("lion", received.get(0).getValue1());
        assertEquals("zebra", received.get(1).getValue1());
    }

    @Test
    void testWindowSimulation() {
        // Simulate a window closing after collecting tuples
        List<Quartet<Long, String, Double, Long>> windowTuples = new ArrayList<>();
        windowTuples.add(new Quartet<>(1L, "lion", 12.0, 0L));
        windowTuples.add(new Quartet<>(2L, "zebra", 18.0, 100L));

        // Simulate window emission
        List<Quartet<Long, String, Double, Long>> emitted = new ArrayList<>(windowTuples);

        assertEquals(2, emitted.size());
        assertEquals("lion", emitted.get(0).getValue1());
        assertEquals("zebra", emitted.get(1).getValue1());
    }

    @Test
    void testEmptyStream() {
        RowStream<Quartet<Long, String, Double, Long>> outStream = new RowStream<>("out");
        List<Quartet<Long, String, Double, Long>> received = new ArrayList<>();

        outStream.addConsumer((stream, tuple, ts) -> received.add(tuple));

        // no tuples sent
        assertTrue(received.isEmpty(), "No tuples should be received from empty stream");
    }
}
