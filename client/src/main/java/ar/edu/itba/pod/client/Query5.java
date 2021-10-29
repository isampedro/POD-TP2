package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.Tree;
import ar.edu.itba.pod.api.combiners.LongSumCombiner;
import ar.edu.itba.pod.api.combiners.Query4CombinerFactory;
import ar.edu.itba.pod.api.mappers.Query5Mapper;
import ar.edu.itba.pod.api.mappers.Query5MapperB;
import ar.edu.itba.pod.api.reducers.LongSumReducerFactory;
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

public class Query5 extends BasicQuery {
    private static final int SUCCESS = 0, FAILURE = 1;
    private final static Logger logger = LoggerFactory.getLogger(Query5.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        parseArguments();
        try {
            if (commonArgsNull() || getArguments(ClientArgsNames.COMMON_NAME) == null
                    || getArguments(ClientArgsNames.NEIGHBOURHOOD) == null)
                throw new IllegalArgumentException(
                        "Address, common name, neighborhood, in directory and out directory must be specified.");
            if (!commonArgsOK()) {
                throw new IllegalArgumentException("City, inPath and outPath must be correctly spelled.");
            }
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            System.exit(FAILURE);
        }

        final HazelcastInstance client = getHazelcastInstance(logger);
        logger.info("Data load finished");

        final JobTracker tracker = client.getJobTracker("query5");

        final IList<Tree> trees = client.getList(HazelcastManager.getTreeNamespace());
        final IMap<String, Long> specieTrees = client.getMap("specie_tree_per_street");
        logger.info("Data retrieved");

        // getting how many trees of the specie there are for each street
        final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(trees);
        final Job<String, Tree> job = tracker.newJob(sourceTrees);
        logger.info("MapReduce Started");
        final ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query5Mapper(getArguments(ClientArgsNames.COMMON_NAME),
                        getArguments(ClientArgsNames.NEIGHBOURHOOD)))
                .combiner(new LongSumCombiner()).reducer(new LongSumReducerFactory()).submit();

        final Map<String, Long> rawResult = future.get();

        specieTrees.putAll(rawResult);
        final KeyValueSource<String, Long> sourceSpeciesPerStreet = KeyValueSource.fromMap(specieTrees);

        final Job<String, Long> finalJob = tracker.newJob(sourceSpeciesPerStreet);
        final ICompletableFuture<Map<Integer, ArrayList<String>>> finalFuture = finalJob.mapper(new Query5MapperB())
                .combiner(new Query4CombinerFactory()).reducer(new Query4ReducerFactory()).submit();
        logger.info("MapReduce Finished");
        final Map<Integer, ArrayList<String>> finalRawResult = finalFuture.get();

        logger.info("Sort Started");
        final List<String> outLines = postProcess(finalRawResult);
        logger.info("Sort Finished");
        final String headers = "GROUP;STREET A; STREET B";
        CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), outLines, headers);
        trees.clear();
        specieTrees.clear();
        System.exit(SUCCESS);
    }

    private static List<String> postProcess(final Map<Integer, ArrayList<String>> rawResult) {
        final List<Integer> tens = new ArrayList<>(
                rawResult.keySet().stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList()));
        final List<String> streetPairs = new ArrayList<>();
        tens.forEach(ten -> {
            List<String> streets = rawResult.get(ten).stream().sorted().collect(Collectors.toList());
            for (int i = 0; i < streets.size(); i++) {
                for (int j = i + 1; j < streets.size(); j++) {
                    streetPairs.add(ten + ";" + streets.get(i) + ";" + streets.get(j));
                }
            }
        });
        return streetPairs;
    }
}
