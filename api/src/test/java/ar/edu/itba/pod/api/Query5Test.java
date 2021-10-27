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

public class Query5Test {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    private static final String COMMON_NAME = "2";
    private static final String NEIGHBOURHOOD = "2";

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

        final JobTracker tracker = h.getJobTracker("query5");

        final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(iTrees);

        //getting how many trees of the specie there are for each street
        Job<String, Tree> job = tracker.newJob(sourceTrees);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query5Mapper(COMMON_NAME, NEIGHBOURHOOD))
                .combiner(new Query5CombinerFactory())
                .reducer(new SumReducerFactoryQuery5())
                .submit();

        Map<String, Long> rawResult = future.get();

        IMap<String, Long> specieTrees = h.getMap("specie_tree_per_streetTESTS");
        specieTrees.putAll(rawResult);
        final KeyValueSource<String, Long> sourceSpeciesPerStreet = KeyValueSource.fromMap(specieTrees);


        final Job<String, Long> finalJob = tracker.newJob(sourceSpeciesPerStreet);
        final ICompletableFuture<Map<Integer, List<String>>> finalFuture = finalJob
                .mapper(new Query5MapperB())
                .combiner(new Query4CombinerFactory())
                .reducer(new Query4ReducerFactory())
                .submit();

        final Map<Integer, List<String>> finalRawResult = finalFuture.get();

        List<String> outLines = postProcess(finalRawResult, COMMON_NAME);

        assertEquals(2, outLines.size());


        assertEquals("11;1",outLines.get(0));
        assertEquals("40;4", outLines.get(1));

    }
    private static List<String> postProcess( final Map<Integer,List<String>> rawResult, String commonName ) {
        final List<String> streetPairs = new LinkedList<>();
        rawResult.forEach((ten, streets) -> {
            for(int i = 0; i < streets.size(); i++) {
                for(int j = i + 1; j < streets.size(); j++) {
                    streetPairs.add(ten + ";" + streets.get(i) + ";" + streets.get(j));
                }
            }
        });

        return streetPairs.stream()
                .sorted()
                .collect(Collectors.toList());
    }

}
