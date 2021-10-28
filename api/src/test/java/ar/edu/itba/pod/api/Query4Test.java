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

public class Query4Test {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    private static final Neighborhood neigh1 = new Neighborhood("Capital", 2);
    private static final Neighborhood neigh2 = new Neighborhood("Ituzaingo", 4);
    private static final Neighborhood neigh3 = new Neighborhood("Nuñes", 4);
    private static final Neighborhood neigh4 = new Neighborhood("Caballito", 4);
    private static final Neighborhood neigh5 = new Neighborhood("Boca", 4);



    private static final List<Tree> trees = Arrays.asList(
            new Tree("a", neigh1, "Gral Wololo"),
            new Tree("a", neigh1, "Gral Wololo"),
            new Tree("c", neigh1, "Gral Wololo"),
            new Tree("d", neigh1, "Gral Wololo"),
            new Tree("d", neigh1, "Gral Wololo"),
            new Tree("e", neigh2, "Av jusepe"),
            new Tree("e", neigh3, "Av jusepeSuelas"),
            new Tree("e", neigh4, "Av jusepeSuelas"),
            new Tree("e", neigh5, "Av jusepeSuelasBoca"));



    // Pares de barrios que registran la misma cantidad de cientos de especies distintas
    @Test
    public void query4Test() throws InterruptedException, ExecutionException {


        HazelcastInstance h = Hazelcast.newHazelcastInstance();

        IList<Tree> iTrees = h.getList("treeTEST");
        iTrees.addAll(trees);

        for (int i = 0; i < 123; i++)
            iTrees.add(new Tree(String.valueOf(i), neigh1, "Gral Wololo"));
        for (int i = 0; i < 100; i++)
            iTrees.add(new Tree(String.valueOf(i), neigh2, "Cpt wolo"));

        final JobTracker tracker = h.getJobTracker("query4");

        final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(iTrees);

        final Job<String, Tree> job = tracker.newJob(sourceTrees);
        final ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new Query3Mapper())
                .combiner(new Query3CombinerFactory())
                .reducer(new Query3ReducerFactory())
                .submit();

        final Map<String, Integer> rawResult = future.get();

        System.out.println(rawResult);

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


        outLines.forEach(System.out::println);

        assertEquals(1,outLines.size());
        assertEquals("100;Capital;Ituzaingo", outLines.get(0));

    }

    private static List<String> postProcess(Map<Integer, ArrayList<String>> rawResult) {

        final List<Integer> hundreds = new ArrayList<>(rawResult.keySet().stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList()));
        final List<String> neighborPairs = new ArrayList<>();
        hundreds.forEach(t -> {
            List<String> streets = rawResult.get(t).stream().sorted().collect(Collectors.toList());
            for (int i = 0; i < streets.size(); i++) {
                for (int j = i + 1; j < streets.size(); j++) {
                    neighborPairs.add(t + ";" + streets.get(i) + ";" + streets.get(j));
                }
            }
        });
        return neighborPairs;

    }
}
