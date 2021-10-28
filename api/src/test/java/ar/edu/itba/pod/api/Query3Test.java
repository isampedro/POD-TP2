package ar.edu.itba.pod.api;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import ar.edu.itba.pod.api.combiners.*;
import ar.edu.itba.pod.api.mappers.*;
import ar.edu.itba.pod.api.reducers.*;

import static org.junit.Assert.assertEquals;

public class Query3Test {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    private static final String N = "2";

    private static final Neighborhood neigh1 = new Neighborhood("Capital", 2);
    private static final Neighborhood neigh2 = new Neighborhood("Ituzaingo", 4);
    private static final Neighborhood neigh3 = new Neighborhood("Nuñez", 4);


    private static final List<Tree> trees = Arrays.asList(
            new Tree("a",neigh1, "Gral Wololo"),
            new Tree("luca",neigh1, "Gral Wololo"),
            new Tree("b",neigh1, "Gral Wololo"),
            new Tree("c",neigh1, "Gral Wololo"),
            new Tree("d",neigh1, "Gral Wololo"),
            new Tree("e",neigh3, "Av juancito"),
            new Tree("f",neigh3, "Av juancito2"),
            new Tree("g",neigh3, "Av juancito3"),
            new Tree("h",neigh2, "Av jusepe"),
            new Tree("i",neigh2, "Av jusepe"));


    // Top n barrios con mayor cantidad de especies distintas
    @Test
    public void query3Test() throws InterruptedException, ExecutionException {

        HazelcastInstance h = Hazelcast.newHazelcastInstance();

        IList<Tree> iTrees = h.getList("treeTEST");
        iTrees.addAll(trees);

        final JobTracker tracker = h.getJobTracker("query3");

        final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(iTrees);

        final Job<String, Tree> job = tracker.newJob(sourceTrees);
        final ICompletableFuture<Map<String, Integer>> future = job.mapper(new Query3Mapper())
                .combiner(new Query3CombinerFactory()).reducer(new Query3ReducerFactory()).submit();

        final Map<String, Integer> rawResult = future.get();

        final List<String> outLines = postProcess(rawResult, Integer.parseInt(N));

        System.out.println(outLines);


        assertEquals("Capital;5",outLines.get(0));
        assertEquals("Nuñez;3", outLines.get(1));

    }

    private static List<String> postProcess(Map<String, Integer> rawResult, int n) {

        Map<String, Integer> result = sortByValue(rawResult);
        List<String> l = new ArrayList<>(result.keySet());

        return l.stream().map(entry -> entry + ";" + result.get(entry)).collect(Collectors.toList()).subList(0, n);
    }

    public static Map<String, Integer> sortByValue(Map<String, Integer> hm) {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Integer>> list = new LinkedList<>(hm.entrySet());

        // Sort the list
        list.sort(new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        // put data from sorted list to hashmap
        HashMap<String, Integer> temp = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }


}
