package relational.examples.animal_examples;

import org.javatuples.Tuple;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.polyflow.api.processing.ContinuousProgram;
import org.streamreasoning.polyflow.api.processing.Task;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.report.ReportImpl;
import org.streamreasoning.polyflow.api.secret.report.strategies.OnStateReady;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeImpl;
import org.streamreasoning.polyflow.api.stream.data.DataStream;
import org.streamreasoning.polyflow.base.contentimpl.factories.StatefulContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.FrameOp;
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import org.streamreasoning.polyflow.base.processing.TaskImpl;

import relational.operatorsimpl.r2s.RelationToStreamjtablesawImpl;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import relational.stream.RowStreamGenerator;

import tech.tablesaw.api.*;

import java.util.Random;

public class polyflow_animalSpeed_thresholdFrame {

    private static final double SPEED_THRESHOLD = 15.0;

    public static void main(String[] args) throws InterruptedException {

        // -------------------------------
        // 1. Animal stream generator
        // -------------------------------
        RowStreamGenerator generator = new RowStreamGenerator() {
            private volatile boolean running = false;

            @Override
            public void startStreaming() {
                running = true;
                Random rand = new Random();
                long start = System.currentTimeMillis();

                String[] species = { "lion", "zebra", "gazelle", "elephant" };
                long id = 1;

                while (running) {
                    String sp = species[(int) ((id - 1) % species.length)];
                    double speed = Math.round((5.0 + rand.nextDouble() * 20.0) * 1000.0) / 1000.0;
                    long ts = System.currentTimeMillis() - start;

                    Tuple t = new org.javatuples.Quartet<>(id, sp, speed, ts);
                    getStream("animals").put(t, ts);

                    id++;
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }

            @Override
            public void stopStreaming() {
                running = false;
            }
        };

        DataStream<Tuple> animals = generator.getStream("animals");
        DataStream<Tuple> out = new RowStream("out");

        // -------------------------------
        // 2. Engine config
        // -------------------------------
        Report report = new ReportImpl();
        report.add(new OnStateReady());

        Tick tick = Tick.TIME_DRIVEN;
        Time time = new TimeImpl(0);

        Table empty = Table.create("animals");

        // -------------------------------
        // 3. Threshold-based content factory
        // -------------------------------
        StatefulContentFactory<Tuple, Tuple, Table> factory = new StatefulContentFactory<>(
                () -> null,

                // extract speed (used as frame control)
                (t, s) -> t.getValue(2),

                // frame continues while speed >= threshold
                s -> ((Double) s) >= SPEED_THRESHOLD,

                // identity
                t -> t,

                // tuple → single-row table
                t -> {
                    Table table = Table.create("animals");
                    LongColumn id = LongColumn.create("id");
                    StringColumn sp = StringColumn.create("species");
                    DoubleColumn speed = DoubleColumn.create("speed");

                    id.append(((Number) t.getValue(0)).longValue());
                    sp.append((String) t.getValue(1));
                    speed.append((Double) t.getValue(2));

                    table.addColumns(id, sp, speed);
                    return table;
                },

                // accumulate rows
                (r1, r2) -> r1.isEmpty() ? r2 : r1.append(r2),

                empty);

        // -------------------------------
        // 4. Frame operator
        // -------------------------------
        StreamToRelationOperator<Tuple, Tuple, Table> frame = new FrameOp<>(tick, time, "speedFrame", factory, report);

        RelationToStreamOperator<Table, Tuple> r2s = new RelationToStreamjtablesawImpl();

        // -------------------------------
        // 5. Task & program
        // -------------------------------
        Task<Tuple, Tuple, Table, Tuple> task = new TaskImpl<>("task");

        task = task.addS2ROperator(frame, animals)
                .addR2SOperator(r2s)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(time);

        task.initialize();

        ContinuousProgram<Tuple, Tuple, Table, Tuple> cp = new ContinuousProgramImpl<>();

        cp.buildTask(task, java.util.List.of(animals), java.util.List.of(out));

        out.addConsumer((s, t, ts) -> System.out.println(" Threshold frame emitted: " + t + " @ " + ts));

        // -------------------------------
        // 6. Run
        // -------------------------------
        Thread th = new Thread(generator::startStreaming);
        th.start();

        Thread.sleep(15_000);
        generator.stopStreaming();
        th.join();
    }
}
