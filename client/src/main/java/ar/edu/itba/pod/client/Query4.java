package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.Tree;
import ar.edu.itba.pod.api.combiners.Query3CombinerFactory;
import ar.edu.itba.pod.api.combiners.Query4CombinerFactory;
import ar.edu.itba.pod.api.mappers.Query3Mapper;
import ar.edu.itba.pod.api.mappers.Query4Mapper;
import ar.edu.itba.pod.api.reducers.Query3ReducerFactory;
import ar.edu.itba.pod.api.reducers.Query4ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query4 extends BasicQuery {
    private static final int SUCCESS = 0, FAILURE = 1;
    private final static Logger logger = LoggerFactory.getLogger(Query4.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        parseArguments();
        try {
            if (commonArgsNull())
                throw new IllegalArgumentException("Address, in directory and out directory must be specified.");
            if (!commonArgsOK()) {
                throw new IllegalArgumentException("City, inPath and outPath must be correctly spelled.");
            }

        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            System.exit(FAILURE);
        }

        HazelcastInstance client = getHazelcastInstance(logger);
        logger.info("Data load finished");

        final JobTracker tracker = client.getJobTracker("query4");

        final IList<Tree> trees = client.getList(HazelcastManager.getTreeNamespace());
        final IMap<String, Integer> differentSpecies = client.getMap("trees_species_per_neighborhood");
        logger.info("Data retrieved");

        // Get neighborhoods with their different tree species
        final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(trees);

        final Job<String, Tree> job = tracker.newJob(sourceTrees);
        logger.info("MapReduce Started");
        final ICompletableFuture<Map<String, Integer>> future = job.mapper(new Query3Mapper())
                .combiner(new Query3CombinerFactory()).reducer(new Query3ReducerFactory()).submit();

        final Map<String, Integer> rawResult = future.get();

        differentSpecies.putAll(rawResult);
        final KeyValueSource<String, Integer> sourceSpeciesPerNeighborhood = KeyValueSource.fromMap(differentSpecies);

        final Job<String, Integer> finalJob = tracker.newJob(sourceSpeciesPerNeighborhood);
        final ICompletableFuture<Map<Integer, ArrayList<String>>> finalFuture = finalJob.mapper(new Query4Mapper())
                .combiner(new Query4CombinerFactory()).reducer(new Query4ReducerFactory()).submit();
        logger.info("MapReduce Finished");
        final Map<Integer, ArrayList<String>> finalRawResult = finalFuture.get();
        final List<String> outLines = postProcess(finalRawResult);
        String headers = "GROUP;NEIGHBOURHOOD A;NEIGHBOURHOOD B";
        CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), outLines, headers);
        trees.clear();
        differentSpecies.clear();
        System.exit(SUCCESS);
    }

    private static List<String> postProcess(Map<Integer, ArrayList<String>> rawResult) {

        final List<Integer> hundreds = new ArrayList<>(
                rawResult.keySet().stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList()));
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
