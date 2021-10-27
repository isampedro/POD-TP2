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

public class Query3Test {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    private static final String N = "2";

    private static final List<Tree> trees = Arrays.asList(
            new Tree("a", new Neighborhood("40", 6), "Gral Wololo"),
            new Tree("a", new Neighborhood("40", 6), "Gral Wololo"),
            new Tree("c", new Neighborhood("40", 6), "Gral Wololo"),
            new Tree("d", new Neighborhood("40", 6), "Gral Wololo"),
            new Tree("e", new Neighborhood("11", 2), "Av jusepe"));

    // Top n barrios con mayor cantidad de especies distintas
    @Test
    public void query1Test() throws InterruptedException, ExecutionException {

        HazelcastInstance h = Hazelcast.newHazelcastInstance();

        IList<Tree> iTrees = h.getList("treeTEST");
        iTrees.addAll(trees);

        final JobTracker tracker = h.getJobTracker("query3");

        final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(iTrees);

        final Job<String, Tree> job = tracker.newJob(sourceTrees);
        final ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new Query3Mapper())
                .combiner(new Query3CombinerFactory())
                .reducer(new Query3ReducerFactory())
                .submit();

        final Map<String, Integer> rawResult = future.get();

        final List<String> outLines = postProcess(rawResult, Integer.parseInt(N));

        assertEquals(2, outLines.size());


        assertEquals("11;1",outLines.get(0));
        assertEquals("40;4", outLines.get(1));

    }
    private static List<String> postProcess(Map<String, Integer> rawResult, int n) {
        List<Map.Entry<String, Integer>> result = rawResult.entrySet().stream()
                .sorted(Comparator.comparing((Function<Map.Entry<String, Integer>, Integer>) Map.Entry::getValue)
                        .thenComparing(Map.Entry::getKey)).collect(Collectors.toList()).subList(0, n-1);

        return result.stream()
                .map(entry -> entry.getKey() + ";" + entry.getValue())
                .collect(Collectors.toList());
    }

}
