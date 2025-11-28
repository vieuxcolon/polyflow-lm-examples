package relational.examples;

import org.javatuples.Quartet;
import org.javatuples.Tuple;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.polyflow.api.processing.ContinuousProgram;
import org.streamreasoning.polyflow.api.processing.Task;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.report.ReportImpl;
import org.streamreasoning.polyflow.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeImpl;
import org.streamreasoning.polyflow.api.stream.data.DataStream;
import org.streamreasoning.polyflow.base.contentimpl.factories.AccumulatorContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.HoppingWindowOpImpl;
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import org.streamreasoning.polyflow.base.processing.TaskImpl;

import relational.operatorsimpl.r2r.CustomRelationalQuery;
import relational.operatorsimpl.r2r.R2RjtablesawJoin;
import relational.operatorsimpl.r2s.RelationToStreamjtablesawImpl;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import relational.stream.RowStreamGenerator;

import tech.tablesaw.api.*;

import java.util.*;

public class polyflow_animalSpeed_accumulate {

    public static void main(String[] args) throws InterruptedException {

        // ---------------------------------------
        // 1. STREAM GENERATOR WITH TWO STREAMS
        // ---------------------------------------
        RowStreamGenerator generator = new RowStreamGenerator() {
            private volatile boolean running = false;

            @Override
            public void startStreaming() {
                running = true;
                Random rand = new Random();
                long start = System.currentTimeMillis();

                String[] species = { "lion", "zebra", "gazelle", "elephant", "dog", "cat" };
                String[] moods = { "happy", "neutral", "angry" };

                long id = 1;

                while (running) {

                    // --- animal tuple ---
                    String sp = species[(int) ((id - 1) % species.length)];
                    int speed = 5 + rand.nextInt(20);
                    long ts = System.currentTimeMillis() - start;

                    Tuple animal = new Quartet<>(id, sp, speed, ts);
                    getStream("animals").put(animal, ts);

                    // --- mood tuple ---
                    String m = moods[rand.nextInt(moods.length)];
                    int moodSpeed = 5 + rand.nextInt(20);
                    Tuple mood = new Quartet<>(id, m, moodSpeed, ts);
                    getStream("moods").put(mood, ts);

                    id++;

                    try {
                        Thread.sleep(200);
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

        // ---------------------------------------
        // 2. STREAM DEFINITIONS
        // ---------------------------------------
        DataStream<Tuple> animals = generator.getStream("animals");
        DataStream<Tuple> moods = generator.getStream("moods");
        DataStream<Tuple> out = new RowStream("out");

        // ---------------------------------------
        // 3. ENGINE CONFIG
        // ---------------------------------------
        Report report = new ReportImpl();
        report.add(new OnWindowClose());
        Time time = new TimeImpl(0);

        AccumulatorContentFactory<Tuple, Tuple, Table> acc = new AccumulatorContentFactory<>(
                t -> t,
                t -> {
                    Table r = Table.create();
                    for (int i = 0; i < t.getSize(); i++) {
                        Object v = t.getValue(i);
                        String col = "c" + (i + 1);

                        if (v instanceof Long) {
                            LongColumn c = r.containsColumn(col) ? (LongColumn) r.column(col) : LongColumn.create(col);
                            c.append((Long) v);
                            if (!r.containsColumn(col))
                                r.addColumns(c);
                        } else if (v instanceof Integer) {
                            IntColumn c = r.containsColumn(col) ? (IntColumn) r.column(col) : IntColumn.create(col);
                            c.append((Integer) v);
                            if (!r.containsColumn(col))
                                r.addColumns(c);
                        } else if (v instanceof String) {
                            StringColumn c = r.containsColumn(col) ? (StringColumn) r.column(col)
                                    : StringColumn.create(col);
                            c.append((String) v);
                            if (!r.containsColumn(col))
                                r.addColumns(c);
                        }
                    }
                    return r;
                },
                (a, b) -> a.isEmpty() ? b : a.append(b),
                Table.create());

        // ---------------------------------------
        // 4. TWO WINDOWS (REQUIRED FOR JOIN)
        // ---------------------------------------
        StreamToRelationOperator<Tuple, Tuple, Table> winAnimals = new HoppingWindowOpImpl<>(Tick.TIME_DRIVEN, time,
                "wAnimals", acc, report, 1000, 1000);

        StreamToRelationOperator<Tuple, Tuple, Table> winMoods = new HoppingWindowOpImpl<>(Tick.TIME_DRIVEN, time,
                "wMoods", acc, report, 1000, 1000);

        // ---------------------------------------
        // 5. JOIN
        // ---------------------------------------
        CustomRelationalQuery q = new CustomRelationalQuery(4, "c1"); // join by ID

        RelationToRelationOperator<Table> join = new R2RjtablesawJoin(q, Arrays.asList("wAnimals", "wMoods"), "joined");

        // ---------------------------------------
        // 6. R2S
        // ---------------------------------------
        RelationToStreamOperator<Table, Tuple> r2s = new RelationToStreamjtablesawImpl();

        // ---------------------------------------
        // 7. TASK
        // ---------------------------------------
        Task<Tuple, Tuple, Table, Tuple> task = new TaskImpl<>("task");

        task = task.addS2ROperator(winAnimals, animals)
                .addS2ROperator(winMoods, moods)
                .addR2ROperator(join)
                .addR2SOperator(r2s)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(time);

        task.initialize();

        ContinuousProgram<Tuple, Tuple, Table, Tuple> cp = new ContinuousProgramImpl<>();

        cp.buildTask(task, Arrays.asList(animals, moods), Collections.singletonList(out));

        out.addConsumer((s, t, ts) -> System.out.println("JOINED → " + t + " @ " + ts));

        // ---------------------------------------
        // 8. START GENERATOR
        // ---------------------------------------
        Thread th = new Thread(generator::startStreaming);
        th.start();

        Thread.sleep(10_000);
        generator.stopStreaming();
        th.join();

        System.out.println(" Finished.");
    }
}
