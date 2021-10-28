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
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import ar.edu.itba.pod.api.combiners.*;
import ar.edu.itba.pod.api.mappers.*;
import ar.edu.itba.pod.api.reducers.*;

public class Query5Test {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    private static final String COMMON_NAME = "luca";
    private static final String NEIGHBOURHOOD = "Capital";

    private static final Neighborhood neigh1 = new Neighborhood("Capital", 2);
    private static final Neighborhood neigh2 = new Neighborhood("Ituzaingo", 4);

    private static final List<Tree> trees = Arrays.asList(new Tree("a", neigh1, "Gral Wololo"),
            new Tree("luca", neigh1, "Gral Wololo"), new Tree("c", neigh1, "Gral Wololo"),
            new Tree("d", neigh1, "Gral Wololo"), new Tree("d", neigh1, "Gral Wololo"),
            new Tree("e", neigh2, "Av jusepe"));

    // Pares de calles de un barrio X que registran la misma cantidad de decenas de
    // árboles de una especie Y
    @Test
    public void query5Test() throws InterruptedException, ExecutionException {

        HazelcastInstance h = Hazelcast.newHazelcastInstance();

        IList<Tree> iTrees = h.getList("treeTEST");
        iTrees.addAll(trees);

        for (int i = 0; i < 25; i++)
            iTrees.add(new Tree("luca", neigh1, "Gral Wololo"));
        for (int i = 0; i < 24; i++)
            iTrees.add(new Tree("luca", neigh1, "Cpt wolo"));
        for (int i = 0; i < 12; i++)
            iTrees.add(new Tree("luca", neigh1, "Bcalle de luca"));
        for (int i = 0; i < 13; i++)
            iTrees.add(new Tree("luca", neigh1, "Acalle ale"));
        for (int i = 0; i < 10; i++)
            iTrees.add(new Tree("luca", neigh1, "Ccalle de pepe"));

        final JobTracker tracker = h.getJobTracker("query5");

        final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(iTrees);

        // getting how many trees of the specie there are for each street
        Job<String, Tree> job = tracker.newJob(sourceTrees);
        ICompletableFuture<Map<String, Long>> future = job.mapper(new Query5Mapper(COMMON_NAME, NEIGHBOURHOOD))
                .combiner(new Query5CombinerFactory()).reducer(new SumReducerFactoryQuery5()).submit();

        Map<String, Long> rawResult = future.get();

        IMap<String, Long> specieTrees = h.getMap("specie_tree_per_streetTESTS");
        specieTrees.putAll(rawResult);
        final KeyValueSource<String, Long> sourceSpeciesPerStreet = KeyValueSource.fromMap(specieTrees);

        final Job<String, Long> finalJob = tracker.newJob(sourceSpeciesPerStreet);
        final ICompletableFuture<Map<Integer, ArrayList<String>>> finalFuture = finalJob.mapper(new Query5MapperB())
                .combiner(new Query4CombinerFactory()).reducer(new Query4ReducerFactory()).submit();

        final Map<Integer, ArrayList<String>> finalRawResult = finalFuture.get();

        List<String> outLines = postProcess(finalRawResult, COMMON_NAME);

        outLines.forEach(System.out::println);
        // assertEquals(2, outLines.size());

        // assertEquals("11;1",outLines.get(0));
        // assertEquals("40;4", outLines.get(1));

    }

    private static List<String> postProcess(final Map<Integer, ArrayList<String>> rawResult, String commonName) {
        final List<Integer> tens = new ArrayList<>(
                rawResult.keySet().stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList()));
        final List<String> streetPairs = new ArrayList<>();
        tens.forEach(t -> {
            List<String> streets = rawResult.get(t).stream().sorted().collect(Collectors.toList());
            for (int i = 0; i < streets.size(); i++) {
                for (int j = i + 1; j < streets.size(); j++) {
                    streetPairs.add(t + ";" + streets.get(i) + ";" + streets.get(j));
                }
            }
        });
        return streetPairs;
    }

}
