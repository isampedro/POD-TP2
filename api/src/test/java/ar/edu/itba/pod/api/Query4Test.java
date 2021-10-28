package ar.edu.itba.pod.api;
/*
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

public class Query4Test {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    private static final List<Tree> trees = Arrays.asList(
            new Tree("a", new Neighborhood("40", 6), "Gral Wololo"),
            new Tree("a", new Neighborhood("40", 6), "Gral Wololo"),
            new Tree("c", new Neighborhood("40", 6), "Gral Wololo"),
            new Tree("d", new Neighborhood("40", 6), "Gral Wololo"),
            new Tree("e", new Neighborhood("11", 2), "Av jusepe"));

    // Pares de barrios que registran la misma cantidad de cientos de especies distintas
    @Test
    public void query1Test() throws InterruptedException, ExecutionException {

        HazelcastInstance h = Hazelcast.newHazelcastInstance();

        IList<Tree> iTrees = h.getList("treeTEST");
        iTrees.addAll(trees);

        final JobTracker tracker = h.getJobTracker("query4");

        final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(iTrees);

        final Job<String, Tree> job = tracker.newJob(sourceTrees);
        final ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new Query3Mapper())
                .combiner(new Query3CombinerFactory())
                .reducer(new Query3ReducerFactory())
                .submit();

        final Map<String, Integer> rawResult = future.get();


        IMap<String, Integer> differentSpecies = h.getMap("trees_species_per_neighborhoodTEST");
        differentSpecies.putAll(rawResult);
        final KeyValueSource<String, Integer> sourceSpeciesPerNeighborhood = KeyValueSource.fromMap(differentSpecies);


        final Job<String, Integer> finalJob = tracker.newJob(sourceSpeciesPerNeighborhood);
        final ICompletableFuture<Map<Integer, ArrayList<String>>> finalFuture = finalJob
                .mapper(new Query4Mapper())
                .combiner(new Query4CombinerFactory())
                .reducer(new Query4ReducerFactory())
                .submit();

        final Map<Integer, ArrayList<String>> finalRawResult = finalFuture.get();
        final List<String> outLines = postProcess(finalRawResult);

//        assertEquals(2, outLines.size());


        //assertEquals("11;1",outLines.get(0));
        //assertEquals("40;4", outLines.get(1));

    }

    private static List<String> postProcess(Map<Integer, ArrayList<String>> rawResult) {
        final List<String> neighborhoodPairs = new LinkedList<>();
        rawResult.forEach((hundred, neighborhoods) -> {
            for(int i = 0; i < neighborhoods.size(); i++) {
                for(int j = i + 1; j < neighborhoods.size(); j++) {
                    neighborhoodPairs.add(hundred + ";" + neighborhoods.get(i) + ";" + neighborhoods.get(j));
                }
            }
        });

        return neighborhoodPairs.stream()
                .sorted()
                .collect(Collectors.toList());
    }
} */
