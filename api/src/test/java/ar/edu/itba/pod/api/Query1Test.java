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

public class Query1Test {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    private static final Neighborhood neigh1 = new Neighborhood("Capital", 2);
    private static final Neighborhood neigh2 = new Neighborhood("Ituzaingo", 4);
    private static final Neighborhood neigh3 = new Neighborhood("Ale", 4);
    private static final Neighborhood neigh4 = new Neighborhood("Boca", 4);

    private static final List<Tree> trees = Arrays.asList(new Tree("a", neigh1, "Gral Wololo"),
            new Tree("b", neigh1, "Gral Wololo"), new Tree("c", neigh1, "Gral Wololo"),
            new Tree("d", neigh1, "Gral Wololo"), new Tree("e", neigh1, "Gral Wololo"),
            new Tree("d", neigh4, "Gral Wololo"), new Tree("e", neigh3, "Gral Wololo"),
            new Tree("f", neigh2, "Av jusepe"));


    // Total de árboles por barrio
    @Test
    public void query1Test() throws InterruptedException, ExecutionException {

        HazelcastInstance h = Hazelcast.newHazelcastInstance();

        IList<Tree> iTrees = h.getList("treeTEST");
        iTrees.addAll(trees);

        final JobTracker tracker = h.getJobTracker("query1");

        KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(iTrees);

        Job<String, Tree> job = tracker.newJob(sourceTrees);
        ICompletableFuture<Map<String, Long>> future = job.mapper(new Query1Mapper())
                .combiner(new LongSumCombiner()).reducer(new LongSumReducerFactory()).submit();

        Map<String, Long> rawResult = future.get();
        List<String> outLines = postProcess(rawResult);

        System.out.println(outLines);

        assertEquals("Capital;5", outLines.get(0));
        assertEquals("Ale;1", outLines.get(1));
        assertEquals("Boca;1", outLines.get(2));
        assertEquals("Ituzaingo;1", outLines.get(3));
        assertEquals(4, outLines.size());
    }

    private static List<String> postProcess(Map<String, Long> rawResult) {

        Map<String, Long> result = sortByValue(rawResult);
        List<String> l = new ArrayList<>(result.keySet());

        return l.stream().map(entry -> entry + ";" + result.get(entry)).collect(Collectors.toList());
    }

    public static Map<String, Long> sortByValue(Map<String, Long> hm) {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Long>> list = new LinkedList<>(hm.entrySet());

        // Sort the list
        list.sort(new Comparator<Map.Entry<String, Long>>() {
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                int compareSpecie = o2.getValue().compareTo(o1.getValue());
                int compareName = o1.getKey().compareTo(o2.getKey());
                return  compareSpecie==0?compareName:compareSpecie;
            }
        });

        // put data from sorted list to hashmap
        HashMap<String, Long> temp = new LinkedHashMap<>();
        for (Map.Entry<String, Long> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }

}
