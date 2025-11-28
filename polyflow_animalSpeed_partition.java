package relational.examples;

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
import org.streamreasoning.polyflow.base.contentimpl.factories.AggregateContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.factories.ContainerContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.HoppingWindowOpImpl;
import org.streamreasoning.polyflow.base.processing.ParallelContinuousProgram;
import org.streamreasoning.polyflow.base.processing.TaskImpl;
import relational.operatorsimpl.r2s.RelationToStreamjtablesawImpl;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import relational.stream.RowStreamGenerator;
import tech.tablesaw.api.*;
import static tech.tablesaw.aggregate.AggregateFunctions.*;

import java.util.ArrayList;
import java.util.List;

public class polyflow_animalSpeed_partition {
    public static void main(String[] args) throws InterruptedException {

        // === Simulated animal stream ===
        RowStreamGenerator generator = new RowStreamGenerator();

        // Tuple: (animalId, species, speed, timestamp)
        DataStream<Tuple> inputStream = generator.getStream("http://wildlife/animals");
        DataStream<Tuple> outStream = new RowStream("out");

        // === Reporting and time management ===
        Report report = new ReportImpl();
        report.add(new OnWindowClose());
        Tick tick = Tick.TIME_DRIVEN;

        Time t1 = new TimeImpl(0);
        Time t2 = new TimeImpl(0);
        Time t3 = new TimeImpl(0);

        // === Define aggregation content ===
        Table empty = Table.create("empty");

        AggregateContentFactory<Tuple, Tuple, Table> aggFactory = new AggregateContentFactory<>(
                t -> t,
                (t) -> {
                    Table r = Table.create("animal_data");

                    for (int i = 0; i < t.getSize(); i++) {
                        Object val = t.getValue(i);
                        String col = "c" + (i + 1);

                        if (val instanceof Long) {
                            if (!r.containsColumn(col))
                                r.addColumns(LongColumn.create(col));
                            r.longColumn(col).append((Long) val);
                        } else if (val instanceof String) {
                            if (!r.containsColumn(col))
                                r.addColumns(StringColumn.create(col));
                            r.stringColumn(col).append((String) val);
                        } else if (val instanceof Double) {
                            if (!r.containsColumn(col))
                                r.addColumns(DoubleColumn.create(col));
                            r.doubleColumn(col).append((Double) val);
                        }
                    }
                    return r;
                },
                (r1, r2) -> r1.isEmpty() ? r2 : r1.append(r2),
                // Group by animal ID (c1) and compute stats on speed (c3)
                (table) -> table.summarize("c3", mean, min, max).by("c1"),
                empty);

        // === Container with key-based partitioning (by animalId) ===
        ContainerContentFactory<Tuple, Tuple, Table, Long> containerFactory = new ContainerContentFactory<>(
                i -> (Long) i.getValue(0), // animalId as key
                w -> null,
                r -> null,
                (r1, r2) -> r1.isEmpty() ? r2 : r1.append(r2),
                Table.create(),
                aggFactory);

        // === Hopping windows: 5s window, slide 2s ===
        StreamToRelationOperator<Tuple, Tuple, Table> s2r1 = new HoppingWindowOpImpl<>(tick, t1, "win1",
                containerFactory, report, 5000, 2000);
        StreamToRelationOperator<Tuple, Tuple, Table> s2r2 = new HoppingWindowOpImpl<>(tick, t2, "win2",
                containerFactory, report, 5000, 2000);
        StreamToRelationOperator<Tuple, Tuple, Table> s2r3 = new HoppingWindowOpImpl<>(tick, t3, "win3",
                containerFactory, report, 5000, 2000);

        // === Relation-to-stream operator ===
        RelationToStreamOperator<Table, Tuple> r2sOp = new RelationToStreamjtablesawImpl();

        // === Define 3 parallel tasks ===
        Task<Tuple, Tuple, Table, Tuple> task1 = new TaskImpl<>("Task1");
        task1 = task1.addS2ROperator(s2r1, inputStream)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(t1);
        task1.initialize();

        Task<Tuple, Tuple, Table, Tuple> task2 = new TaskImpl<>("Task2");
        task2 = task2.addS2ROperator(s2r2, inputStream)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(t2);
        task2.initialize();

        Task<Tuple, Tuple, Table, Tuple> task3 = new TaskImpl<>("Task3");
        task3 = task3.addS2ROperator(s2r3, inputStream)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(t3);
        task3.initialize();

        // === Continuous program setup ===
        List<DataStream<Tuple>> inputs = List.of(inputStream);
        List<DataStream<Tuple>> outputs = List.of(outStream);

        ContinuousProgram<Tuple, Tuple, Table, Tuple> cp = new ParallelContinuousProgram<>(i -> (Long) i.getValue(0),
                3);

        cp.buildTask(task1, inputs, outputs);
        cp.buildTask(task2, inputs, outputs);
        cp.buildTask(task3, inputs, outputs);

        // === Output consumer ===
        outStream.addConsumer((out, el, ts) -> System.out.println("Animal speed summary: " + el + " @ " + ts));

        // === Start streaming ===
        generator.startStreaming();
    }
}
