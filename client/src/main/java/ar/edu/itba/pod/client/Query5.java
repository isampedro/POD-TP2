package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.Tree;
import ar.edu.itba.pod.api.combiners.Query4CombinerFactory;
import ar.edu.itba.pod.api.combiners.Query5CombinerFactory;
import ar.edu.itba.pod.api.mappers.Query5Mapper;
import ar.edu.itba.pod.api.mappers.Query5MapperB;
import ar.edu.itba.pod.api.reducers.Query4ReducerFactory;
import ar.edu.itba.pod.api.reducers.SumReducerFactoryQuery5;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query5 extends BasicQuery {
    private static final int SUCCESS = 0, FAILURE = 1;

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        parseArguments();

        try {
            if (commonArgsNull() || getArguments(ClientArgsNames.COMMON_NAME) == null
                    || getArguments(ClientArgsNames.NEIGHBOURHOOD) == null)
                throw new IllegalArgumentException("Address, in directory and out directory must be specified.");
            if (!commonArgsOK()) {
                throw new IllegalArgumentException("City, inPath and outPath must be correctly spelled.");
            }
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            System.exit(FAILURE);
        }

        HazelcastInstance client = getHazelcastInstance();
        final JobTracker tracker = client.getJobTracker("query5");

        IList<Tree> trees = client.getList(HazelcastManager.getTreeNamespace());

        // getting how many trees of the specie there are for each street
        KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(trees);
        Job<String, Tree> job = tracker.newJob(sourceTrees);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query5Mapper(getArguments(ClientArgsNames.COMMON_NAME),
                        getArguments(ClientArgsNames.NEIGHBOURHOOD)))
                .combiner(new Query5CombinerFactory()).reducer(new SumReducerFactoryQuery5()).submit();

        Map<String, Long> rawResult = future.get();

        IMap<String, Long> specieTrees = client.getMap("specie_tree_per_street");
        specieTrees.putAll(rawResult);
        final KeyValueSource<String, Long> sourceSpeciesPerStreet = KeyValueSource.fromMap(specieTrees);

        final Job<String, Long> finalJob = tracker.newJob(sourceSpeciesPerStreet);
        final ICompletableFuture<Map<Integer, ArrayList<String>>> finalFuture = finalJob.mapper(new Query5MapperB())
                .combiner(new Query4CombinerFactory()).reducer(new Query4ReducerFactory()).submit();

        final Map<Integer, ArrayList<String>> finalRawResult = finalFuture.get();

        List<String> outLines = postProcess(finalRawResult);

        String headers = "GROUP;STREET A; STREET B";
        CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), outLines, headers);
        System.exit(SUCCESS);
    }

    private static List<String> postProcess( final Map<Integer, ArrayList<String>> rawResult) {
        final List<Integer> tens = new ArrayList<>(rawResult.keySet().stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList()));
        final List<String> streetPairs = new ArrayList<>();
        tens.forEach(ten-> {
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
