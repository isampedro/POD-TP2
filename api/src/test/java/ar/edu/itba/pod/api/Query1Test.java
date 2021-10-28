package ar.edu.itba.pod.api;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import ar.edu.itba.pod.api.combiners.*;
import ar.edu.itba.pod.api.mappers.*;
import ar.edu.itba.pod.api.*;
import ar.edu.itba.pod.api.reducers.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class Query1Test {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    private static final Neighborhood neigh1 =new Neighborhood("Capital", 2);
    private static final Neighborhood neigh2 =new Neighborhood("Ituzaingo", 4);

    private static final List<Tree> trees = Arrays.asList(
            new Tree("a",neigh1, "Gral Wololo"),
            new Tree("b",neigh1, "Gral Wololo"),
            new Tree("c",neigh1, "Gral Wololo"),
            new Tree("d",neigh1, "Gral Wololo"),
            new Tree("e",neigh1, "Gral Wololo"),
            new Tree("f",neigh2, "Av jusepe"));
    //Total de árboles por barrio
    @Test
    public void query1Test() throws InterruptedException, ExecutionException {

        HazelcastInstance h = Hazelcast.newHazelcastInstance();

        IList<Tree> iTrees = h.getList("treeTEST");
        iTrees.addAll(trees);

        final JobTracker tracker = h.getJobTracker("query1");

        KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(iTrees);

        Job<String, Tree> job = tracker.newJob(sourceTrees);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query1Mapper())
                .combiner(new Query1CombinerFactory())
                .reducer(new SumReducerFactory())
                .submit();

        Map<String, Long> rawResult = future.get();
        List<String> outLines = postProcess(rawResult);

        assertEquals(2, outLines.size());


        assertEquals("11;1",outLines.get(0));
        assertEquals("40;4", outLines.get(1));
        assertEquals(2, outLines.size());
    }

    private static List<String> postProcess(Map<String, Long> rawResult) {
        List<Map.Entry<String, Long>> result = rawResult.entrySet().stream()
                .sorted(Comparator.comparing((Function<Map.Entry<String, Long>, Long>) Map.Entry::getValue)
                        .thenComparing(Map.Entry::getKey)).collect(Collectors.toList());

        return result.stream()
                .map(entry -> entry.getKey() + ";" + entry.getValue())
                .collect(Collectors.toList());
    }

}
