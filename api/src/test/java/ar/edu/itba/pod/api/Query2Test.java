package ar.edu.itba.pod.api;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.text.DecimalFormat;

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

public class Query2Test {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    private static final Neighborhood neigh1 =new Neighborhood("Capital", 9);
    private static final Neighborhood neigh2 =new Neighborhood("Ituzaingo", 2);

    private static final List<Tree> trees = Arrays.asList(
            new Tree("a",neigh1, "Gral Wololo"),
            new Tree("a",neigh1, "Gral Wololo"),
            new Tree("a",neigh1, "Gral Wololo"),
            new Tree("d",neigh1, "Gral Wololo"),
            new Tree("d",neigh1, "Gral Wololo"),
            new Tree("e",neigh2, "Av jusepe"));

    // Para cada barrio, la especie con mayor cantidad de árboles por habitante
    @Test
    public void query2Test() throws InterruptedException, ExecutionException {

        HazelcastInstance h = Hazelcast.newHazelcastInstance();

        IList<Tree> iTrees = h.getList("treeTEST");
        iTrees.addAll(trees);

        final JobTracker tracker = h.getJobTracker("query2");

        final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(iTrees);

        final Job<String, Tree> job = tracker.newJob(sourceTrees);
        final ICompletableFuture<Map<Pair<String, String>, Double>> future = job
                .mapper(new Query2Mapper())
                .combiner(new Query2CombinerFactory())
                .reducer(new SumReducerFactoryQuery2())
                .submit();

        final Map<Pair<String,String>, Double> rawResult = future.get();

        List<String> outLines = postProcess(rawResult);

        System.out.println(outLines);

        assertEquals(2, outLines.size());

        assertEquals("Capital;a;0.33",outLines.get(0));
        assertEquals("Ituzaingo;e;0.50" , outLines.get(1));

    }

    private static List<String> postProcess(Map<Pair<String,String>, Double> rawResult) {
        final Map<String, SortedSet<Pair<Double,String>>> finalMap = new HashMap<>();
        rawResult.forEach( (k, v) -> {
            finalMap.putIfAbsent(k.fst,new TreeSet<>());
            finalMap.get(k.fst).add(new Pair<>(v,k.snd));
        });

        List<String> orderKeys = finalMap.keySet().stream().sorted().collect(Collectors.toList());


        return orderKeys.stream()
                .map(entry -> {
                    String treeName = finalMap.get(entry).first().snd;
                    Double value = finalMap.get(entry).first().fst;
                    DecimalFormat f = new DecimalFormat("0.00");

                    return entry + ";" + treeName + ";" + f.format(value);
                })
                .collect(Collectors.toList());
    }


}
