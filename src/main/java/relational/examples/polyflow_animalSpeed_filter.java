package relational.examples;

import org.javatuples.Quartet;
import org.javatuples.Tuple;
import org.streamreasoning.polyflow.api.enums.Tick;
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
import org.streamreasoning.polyflow.base.contentimpl.factories.FilterContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.HoppingWindowOpImpl;
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import org.streamreasoning.polyflow.base.processing.TaskImpl;
import relational.operatorsimpl.r2s.RelationToStreamjtablesawImpl;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import relational.stream.RowStreamGenerator;
import tech.tablesaw.api.*;

import java.util.Random;
import java.util.List;
import java.util.ArrayList;

public class polyflow_animalSpeed_filter {

    public static void main(String[] args) throws InterruptedException {

        // === Generator ===
        RowStreamGenerator generator = new RowStreamGenerator() {
            private final String[] speciesList = { "lion", "zebra", "gazelle", "elephant", "dog", "cat" };
            private final Random rand = new Random();

            @Override
            public void startStreaming() {
                long ts = 0;
                long animalId = 1;
                while (!Thread.currentThread().isInterrupted() && ts < 20_000) {
                    String sp = speciesList[(int) ((animalId - 1) % speciesList.length)];
                    double speed = 5 + rand.nextDouble() * 20; // 5–25 m/s
                    Quartet<Long, String, Double, Long> tuple = new Quartet<>(animalId, sp, speed, ts);

                    // Alternate streams
                    if (animalId % 2 == 0)
                        getStream("http://wildlife/animals2").put(tuple, ts);
                    else
                        getStream("http://wildlife/animals1").put(tuple, ts);

                    animalId++;
                    ts += 200;
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        };

        // === Input streams from generator ===
        DataStream<Tuple> inputStream1 = generator.getStream("http://wildlife/animals1");
        DataStream<Tuple> inputStream2 = generator.getStream("http://wildlife/animals2");

        // === Output stream ===
        RowStream<Tuple> outStream = new RowStream<>("out");

        // === Engine config ===
        Report report = new ReportImpl();
        report.add(new OnWindowClose());
        Tick tick = Tick.TIME_DRIVEN;
        Time timeInstance = new TimeImpl(0);

        // === Filter content: only speed > 15 ===
        FilterContentFactory<Tuple, Tuple, Table> filterFactory = new FilterContentFactory<>(
                t -> t,
                t -> {
                    Table table = Table.create("animal_data");
                    table.addColumns(
                            LongColumn.create("id"),
                            StringColumn.create("species"),
                            DoubleColumn.create("speed"),
                            LongColumn.create("ts"));

                    table.longColumn("id").append((Long) t.getValue(0));
                    table.stringColumn("species").append((String) t.getValue(1));
                    table.doubleColumn("speed").append((Double) t.getValue(2));
                    table.longColumn("ts").append((Long) t.getValue(3));
                    return table;
                },
                (t1, t2) -> t1.isEmpty() ? t2 : t1.append(t2),
                Table.create(),
                t -> {
                    double speed = (Double) t.getValue(2);
                    if (speed <= 15) {
                        System.out.printf("Not adding element [%d, %s, %.3f, %d] to window content%n",
                                t.getValue(0), t.getValue(1), speed, t.getValue(3));
                        return false;
                    }
                    return true;
                });

        // === Window operators ===
        StreamToRelationOperator<Tuple, Tuple, Table> s2r1 = new HoppingWindowOpImpl<>(tick, timeInstance, "win1",
                filterFactory, report, 5000, 2000);
        StreamToRelationOperator<Tuple, Tuple, Table> s2r2 = new HoppingWindowOpImpl<>(tick, timeInstance, "win2",
                filterFactory, report, 5000, 2000);

        // === Relation to Stream operator ===
        RelationToStreamOperator<Table, Tuple> r2s = new RelationToStreamjtablesawImpl();

        // === Task ===
        Task<Tuple, Tuple, Table, Tuple> task = new TaskImpl<>("animalTask");
        task = task.addS2ROperator(s2r1, inputStream1)
                .addS2ROperator(s2r2, inputStream2)
                .addR2SOperator(r2s)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(timeInstance);
        task.initialize();

        // === Continuous Program ===
        ContinuousProgram<Tuple, Tuple, Table, Tuple> cp = new ContinuousProgramImpl<>();
        List<DataStream<Tuple>> inputStreams = new ArrayList<>();
        inputStreams.add(inputStream1);
        inputStreams.add(inputStream2);
        List<DataStream<Tuple>> outputStreams = new ArrayList<>();
        outputStreams.add(outStream);
        cp.buildTask(task, inputStreams, outputStreams);

        // === Output consumer ===
        outStream.addConsumer((stream, el, ts) -> {
            System.out.printf("[%d, %s, %.3f, %d] @ %d%n",
                    el.getValue(0),
                    el.getValue(1),
                    el.getValue(2),
                    el.getValue(3),
                    ts);
        });

        // === Start generator ===
        Thread genThread = new Thread(generator::startStreaming);
        genThread.start();

        // Let it run for 20s
        Thread.sleep(20_000);

        genThread.interrupt();
        genThread.join();
    }

        
}
